from math import ceil
import numpy as np
import scipy as sp
import xlwings as xw


def util_build_chunks(lst, size):
    return list(map(
        lambda x: lst[x * size:x * size + size], list(range(0, ceil(len(lst) / size)))))


def xw_load_workbooks(filepath):
    return xw.Book(filepath)


def xw_activate_workbook(func):
    def wrapper(workbook_to_activate):
        workbook_to_activate.activate()
        return func(workbook_to_activate)
    return wrapper


@xw_activate_workbook
def xw_get_selection(workbook):
    return workbook.selection.get_address(False, False, True, False)


def stat_min_max_norm(x):
    return (x - np.min(x)) / (np.max(x) - np.min(x))


def stat_standardization(x):
    return (x - np.mean(x)) / np.std(x)


def stat_gen_dist_uniform(start, end, num, loc=0, scale=1):
    from scipy.stats import uniform

    x = np.linspace(start, end, num)
    x_n = stat_min_max_norm(x)

    return x, uniform.pdf(x_n, loc, scale)


def stat_gen_dist_normal(start, end, num, loc=0, scale=1):
    from scipy.stats import norm

    x = np.linspace(start, end, num)
    x_s = stat_standardization(x)

    return x, norm.pdf(x_s, loc, scale)


def stat_gen_dist_exponential(start, end, num, loc=0, scale=1):
    from scipy.stats import expon

    x = np.linspace(start, end, num)
    x_n = stat_min_max_norm(x)

    return x, expon.pdf(x_n, loc, scale)


# TODO poisson, beta


if __name__ == '__main__':
    # # test_file = 'D:/Localhome/sekim/OneDrive - ZF Friedrichshafen AG/Desktop/NPV concept.xlsx'
    # test_file = 'C:/Users/su79e/Desktop/test.xlsx'
    #
    # wb = xw_load_workbooks(test_file)
    # print(xw_get_selection(wb))
    #
    print(stat_gen_dist_uniform(500, 1500, 30).shape)

