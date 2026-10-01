from dataclasses import dataclass,replace
from decimal import Decimal
from threading import RLock
@dataclass(frozen=True)
class Capacity: leg_id:str; available:Decimal; version:int=0
@dataclass(frozen=True)
class Receipt: booking_id:str; account:str; stream:str; reserved:dict; entries:tuple; event:object
class Book:
    def __init__(self,rows): self.rows={r.leg_id:r for r in rows};self.lock=RLock()
    def snapshot(self,ids):
        with self.lock:return {i:self.rows[i] for i in ids}
    def reserve_atomic(self,requests,versions):
        if set(requests)!=set(versions): raise ValueError('key mismatch')
        with self.lock:
            current={}
            for leg,q in requests.items():
                if q<=0: raise ValueError('positive quantity required')
                r=self.rows[leg]
                if r.version!=versions[leg]: raise RuntimeError('version conflict')
                if r.available<q: raise ValueError('insufficient capacity')
                current[leg]=r
            result={}
            for leg,q in requests.items():
                r=current[leg]
                n=replace(r,available=r.available-q,version=r.version+1)
                self.rows[leg]=n
                result[leg]=n
            return result
    def release_atomic(self,releases):
        with self.lock:
            result={}
            for leg,q in releases.items():
                if q<=0: raise ValueError('positive quantity required')
                r=self.rows[leg]
                n=replace(r,available=r.available+q,version=r.version+1)
                self.rows[leg]=n
                result[leg]=n
            return result
def commit_booking(book,ledger,events,booking_id,requests,versions,account,stream=None):
    """Commit a multi-leg booking across capacity, container ledger and event
    stream as one unit. Every leg is pre-validated and reserved in a single
    atomic step; the ledger postings and the event append that follow are
    covered by a compensating release in reverse order, so any failure -
    including a version race - leaves no partial occupation, no dangling
    entries and no event behind. booking_id is the idempotency key and must
    be unique per booking attempt."""
    if not booking_id: raise ValueError('booking_id required')
    if not account: raise ValueError('account required')
    if not requests: raise ValueError('empty booking')
    if set(requests)!=set(versions): raise ValueError('key mismatch')
    quantities={}
    for leg,q in requests.items():
        q=Decimal(str(q))
        if q<=0: raise ValueError('positive quantity required')
        quantities[leg]=q
    refs={leg:f'{booking_id}:{leg}' for leg in quantities}
    known={(e.account,e.reference) for e in ledger.entries()}
    if any((account,ref) in known for ref in refs.values()): raise ValueError('duplicate idempotency reference')
    stream=stream or f'booking:{booking_id}'
    reserved=None;posted=[]
    try:
        reserved=book.reserve_atomic(quantities,versions)
        for leg in quantities: posted.append(ledger.post(account,refs[leg],quantities[leg],'booking'))
        event=events.append(stream,len(events.read(stream)),'booking_committed',{
            'booking_id':booking_id,'account':account,
            'legs':{leg:str(quantities[leg]) for leg in sorted(quantities)},
            'versions':{leg:reserved[leg].version for leg in sorted(reserved)}})
        return Receipt(booking_id,account,stream,reserved,tuple(posted),event)
    except Exception:
        for entry in reversed(posted): ledger.reverse(entry.sequence,entry.reference+':reversal','booking_compensation')
        if reserved is not None: book.release_atomic(quantities)
        raise
