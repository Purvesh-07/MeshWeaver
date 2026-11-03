import math

def heavy(n):
    return sum(math.sqrt(i) for i in range(n))
