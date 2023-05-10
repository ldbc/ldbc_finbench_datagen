import math
import random
from collections import Counter

import numpy as np
from matplotlib import pyplot as plt
from scipy.integrate import quad

data_size = 10000
ind_alphas = [109539041.821, 78379700.038, 133908623.887]
outd_alphas = [20186572.914, 14153912.686, 20194472.855]
ind_betas = np.array([-2.319, -2.319, -2.085])
outd_betas = np.array([-1.720, -1.719, -1.720])
start_degree = 1
max_degree = 1000


def calc_integral():
    a = ind_alphas[0]
    b = ind_betas[0]

    def avg_degree(n):
        return math.pow(n, 0.512 - 0.028 * math.log10(n))

    def powerlaw_func(x):
        return a * np.power(x, b)

    areas, _ = quad(powerlaw_func, start_degree, max_degree)
    print(np.power((np.power(max_degree, b + 1) + 9 * np.power(0, b + 1)) / 10, 1 / (b + 1)))


# According to https://mathworld.wolfram.com/RandomNumber.html
# The formula to transform uniform distribution to powerlaw distribution is:
# x = [(x1^(n+1) - x0^(n+1))*y + x0^(n+1)]^(1/(n+1))
def draw_powerlaw():
    beta = np.average(outd_betas)

    def powerlaw_func(y):
        return (int)(np.power(
            (np.power(max_degree, beta + 1) - np.power(start_degree, beta + 1)) * y + np.power(start_degree, beta + 1),
            1 / (beta + 1)))

    degree = [powerlaw_func(random.uniform(0, 1)) for _ in range(0, data_size)]
    freq = Counter(degree).most_common()
    degrees = []
    counts = []
    for deg, count in freq:
        degrees.append(deg)
        counts.append(count)
    plt.scatter(degrees, counts)
    plt.loglog()
    plt.plot()
    plt.show()


if __name__ == "__main__":
    draw_powerlaw()
