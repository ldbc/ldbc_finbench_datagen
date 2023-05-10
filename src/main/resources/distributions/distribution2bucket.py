import math

import numpy as np
from scipy.integrate import quad

data_size = 180000000.0
ind_alphas = [109539041.821, 78379700.038, 133908623.887]
ind_betas = [-2.319, -2.319, -2.085]
outd_alphas = [20186572.914, 14153912.686, 20194472.855]
outd_betas = [-1.720, -1.719, -1.720]
start_degree = 0.01
max_degree = 10e4

a = ind_alphas[0]
b = ind_betas[0]

def avg_degree(n):
    return math.pow(n, 0.512 - 0.028 * math.log10(n))

def powerlaw_func(x):
    return a * np.power(x, b)




areas,_ = quad(powerlaw_func, start_degree, max_degree)



print(np.power((np.power(max_degree,b+1)+9*np.power(0,b+1))/10, 1/(b+1)))
