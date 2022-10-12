# coding:utf-8
import os
import numpy as np
import math
import matplotlib.pyplot as plt

#filename = "in_degree_distribution.txt"
filename = "out_degree_distribution.txt"

def format_digital(n):
    if n > 1e9:
        return '{:.2f}b'.format((n*1.0/1e9))
    if n > 1e6:
        return '{:.2f}m'.format((n*1.0/1e6))
    if n > 1e3:
        return '{:.2f}k'.format((n*1.0/1e3))
    return n

if __name__ == "__main__":
    fin = open(filename, 'r')
    x = []
    y = []
    for line in fin:
        pair = line.strip().split()
        pair = [int(pair[0]), int(pair[1])]
        if pair[0] == 0:
            continue
        x.append(pair[0])
        y.append(pair[1])
    #x = [math.log(v) for v in x]
    #y = [math.log(v) for v in y]
    plt.scatter(x, y)
    plt.plot()
    plt.loglog()

    plt.title(filename)
    plt.text(x[0], y[0], '({},{})'.format(format_digital(x[0]),format_digital(y[0])))
    plt.text(x[-1], y[-1], '({},{})'.format(format_digital(x[-1]),format_digital(y[-1])))
    plt.xlabel("degree")
    plt.ylabel("count")

    plt.savefig(filename + '.png')
    plt.show()