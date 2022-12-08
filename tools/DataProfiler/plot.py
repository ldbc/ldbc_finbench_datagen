# coding:utf-8
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit


def format_digital(n):
    if n > 1e9:
        return '{:.2f}b'.format((n * 1.0 / 1e9))
    if n > 1e6:
        return '{:.2f}m'.format((n * 1.0 / 1e6))
    if n > 1e3:
        return '{:.2f}k'.format((n * 1.0 / 1e3))
    return n


def plot_degree(dir, name, show=False, delimiter=" "):
    x = []
    y = []
    with open(os.path.join(dir, name + ".txt"), 'r') as fin:
        for line in fin:
            pair = line.strip().split(delimiter)
            pair = [int(pair[0]), int(pair[1])]
            if pair[0] == 0:
                continue
            x.append(pair[0])
            y.append(pair[1])
    plt.scatter(x, y)
    plt.plot()
    plt.loglog()  # Draw in log scaling

    plt.title(name)
    plt.text(x[0], y[0], '({},{})'.format(
        format_digital(x[0]), format_digital(y[0])))
    plt.text(x[-1], y[-1],
             '({},{})'.format(format_digital(x[-1]), format_digital(y[-1])))
    plt.xlabel("degree")
    plt.ylabel("count")

    plt.savefig(os.path.join(dir, name + ".png"))
    if show:
        plt.show()
    plt.clf()

# Not Required. Caculate the avg in/out degree if you need
def cal_avg_degree(dir, name):
    deg_sum = 0
    count_sum = 0
    with open(os.path.join(dir, name + ".txt"), 'r') as fin:
        for line in fin:
            pair = line.strip().split()
            if int(pair[0]) == 0:
                continue
            deg_sum += int(pair[0]) * int(pair[1])
            count_sum += int(pair[1])
    with open(os.path.join(dir, name + "_avg_degree.txt"), 'w') as fout:
        fout.write(
            "{}: Sum deg {}, sum count {}, avg deg {}\n".format(name, deg_sum, count_sum, deg_sum * 1.0 / count_sum))
        fout.close()


def powerlaw_regression(dir, name, show=False, delimiter=" "):
    x = []
    y = []
    with open(os.path.join(dir, name + ".txt"), 'r') as fin:
        for line in fin:
            pair = line.strip().split(delimiter)
            pair = [int(pair[0]), int(pair[1])]
            if pair[0] == 0:
                continue
            x.append(pair[0])
            y.append(pair[1])

    def power_law(x, a, b):
        return a * np.power(x, b)

    output_name = name + "_regression"
    pars, cov = curve_fit(f=power_law, xdata=x, ydata=y, p0=[0, 0], bounds=[-np.inf, np.inf])
    # stdevs = np.sqrt(np.diag(cov)) # Get the standard deviations of the parameters
    with open(os.path.join(dir, output_name + ".txt"), 'w') as fout:
        fout.write("formula: y = alpha * (x^beta)\n")
        fout.write("alpha: {}\n".format(pars[0]))
        fout.write("beta: {}\n".format(pars[1]))
        fout.close()

    y_regression = power_law(x, *pars)
    y_regression_ceil = np.ceil(power_law(x, *pars))

    ax = plt.gca()
    ax.loglog(x, y, color="red", label="Raw Distribution")  # Draw in log scaling
    ax.loglog(x, y_regression, color='blue', label="Fitted Distribution")
    ax.loglog(x, y_regression_ceil, color='green', label="Fitted Distribution with Ceiling")
    ax.legend()
    plt.title(output_name)
    plt.text(x[0], y[0], '({},{})'.format(
        format_digital(x[0]), format_digital(y[0])))
    plt.text(x[-1], y[-1],
             '({},{})'.format(format_digital(x[-1]), format_digital(y[-1])))
    plt.xlabel("degree")
    plt.ylabel("count")
    plt.savefig(os.path.join(dir, output_name + ".png"))
    if show:
        plt.show()
    plt.clf()


if __name__ == "__main__":
    log_dir = sys.argv[1]
    plot_degree(log_dir, "in_degree_dist")
    plot_degree(log_dir, "out_degree_dist")
    powerlaw_regression(log_dir, "in_degree_dist")
    powerlaw_regression(log_dir, "out_degree_dist")
    for topid in range(1,6):
        plot_degree(log_dir, "hub_outdeg_{}".format(topid), delimiter=",")
        powerlaw_regression(log_dir, "hub_outdeg_{}".format(topid), delimiter=",")
        plot_degree(log_dir, "hub_indeg_{}".format(topid), delimiter=",")
        powerlaw_regression(log_dir, "hub_indeg_{}".format(topid), delimiter=",")
