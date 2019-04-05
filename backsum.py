# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 18:11:49 2019

@author: wb390262
"""
from timeit import timeit
from numpy import cumsum
import numpy as np
        
array = [1, 2, 3, 4] * 100
nparr = np.array(array)
n = 10000

# Subtracting from sum :: @Sarcoma
# timeit: 0.6
def subFromSum(arr):
    total = sum(arr)
    result = []
    for value in arr:
        result.append(total)
        total -= value
    return result
print("subFromSum    ", timeit(lambda :subFromSum(array), number=n))


# Procedure for-loop assigning list items
# timeit: 0.07
def procedural(arr): 
    result = arr.copy()
    total  = 0
    index  = len(arr)-1 
    for value in reversed(arr):
        total += value
        result[index] = total
        index -= 1
    return result
print("procedural    ", timeit(lambda :procedural(array), number=n))

# generator :: @Sarcoma
# timeit: 0.08
def gen(a):
    r = 0
    for x in a:
        r += x
        yield r
def generator(arr):
    return [*gen(arr[::-1])][::-1]
print("generator     ", timeit(lambda : generator(array), number=n))


# recursive concatenation
# timeit: 0.11
def recursive(arr,size=None):
    size = (size or len(arr))
    value = arr[size-1]
    if size == 1 : return [value]
    previous = recursive(arr,size-1)
    return previous + [value+previous[-1]]
#print("recursive     ", timeit(lambda :recursive(array), number=n))

# iterative array sum()  :: @JosepJoestar
# timeit: 0.14
def arraySum(arr):
    s = []
    for i in range(len(arr)):
        s.append(sum(arr[i:]))
    return s

#print("arraySum      ", timeit(lambda : arraySum(array), number=n))

# list comprehension :: @student
# timeit: 0.13
def listComp(arr):
    return [sum(arr[i:]) for i in range(len(arr))]
#print("listComp      ", timeit(lambda : listComp(array), number=n))

# accumulate() function form itertools
# timeit: 0.14
def iterAccumulate(arr): 
    from itertools import accumulate
    return list(accumulate(arr[::-1]))[::-1]
print("iterAccumulate", timeit(lambda : iterAccumulate(array), number=n))
print("iterAccumulate2", timeit(lambda : iterAccumulate(nparr), number=n))

# assigning list items using functools' reduce() function
# timeit: 0.18
def funcReduce(arr):
    from functools import reduce
    return reduce(lambda a,v: a + [a[-1]-v], arr[1:], [sum(arr)])
print("funcReduce    ", timeit(lambda : funcReduce(array), number=n))

# npAccumulate() function form numpy :: @ user2699
# timeit: 0.24
def mpAccumulate(arr): 
    return np.add.accumulate(arr[::-1])[::-1]
print("npAccumulate  ", timeit(lambda : mpAccumulate(nparr), number=n))

# numpy's cumsum() function
# timeit: 0.55
def npCumSum(arr): 
    return cumsum(arr[::-1])[::-1]
print("npCumSum      ", timeit(lambda : npCumSum(nparr), number=n))

def np_cum_sum2(arr):
    return np.flip(cumsum(np.flip(arr)))
print("npCumSum2      ", timeit(lambda : np_cum_sum2(nparr), number=n))


# conceptual matrix operations (using numpy)
# timeit: 2.05
def npSumTriu(arr): 
    return np.sum(np.triu(arr),1)
#print("npSumTriu     ", timeit(lambda : npSumTriu(array), number=n))


