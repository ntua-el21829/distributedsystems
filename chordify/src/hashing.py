import hashlib

MAX_ID = 2**160

def sha1_int(value: str) -> int:
   h = hashlib.sha1(value.encode()).hexdigest()
   return int(h, 16)

def in_interval(x, a, b):
   """
   Returns True if x ∈ (a, b] in circular ID space.
   """
   if a == b:
       # degenerate interval, treat as full ring (useful when only 1 node)
       return True

   if a < b:
       return a < x <= b
   else:
       # wrap around
       return x > a or x <= b
