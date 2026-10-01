from dataclasses import dataclass,replace
from decimal import Decimal
from threading import RLock
@dataclass(frozen=True)
class Capacity: leg_id:str; available:Decimal; version:int=0
class Book:
    def __init__(self,rows): self.rows={r.leg_id:r for r in rows};self.lock=RLock()
    def snapshot(self,ids):
        with self.lock:return {i:self.rows[i] for i in ids}
    def reserve_atomic(self,requests,versions):
        if set(requests)!=set(versions): raise ValueError('key mismatch')
        with self.lock:
            current={}
            # Phase 1: validate every leg before touching any row.
            for leg,q in requests.items():
                if leg not in self.rows: raise KeyError(leg)
                if q<=0: raise ValueError('positive quantity required')
                r=self.rows[leg]
                if r.version!=versions[leg]: raise RuntimeError('version conflict')
                if r.available<q: raise ValueError('insufficient capacity')
                current[leg]=r
            # Phase 2: all legs checked together; apply the whole batch.
            result={}
            for leg,q in requests.items():
                r=current[leg]
                n=replace(r,available=r.available-q,version=r.version+1)
                self.rows[leg]=n
                result[leg]=n
            return result
