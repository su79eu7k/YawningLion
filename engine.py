import numpy as np
import scipy as sp
import xlwings as xw


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


def min_max_normalization(x):
    return (x - np.min(x)) / (np.max(x) - np.min(x))


def gen_dist_uniform(start, end, num):
    from scipy.stats import uniform

    x = np.linspace(start, end, num)
    x_n = min_max_normalization(x)

    return np.vstack([x, uniform.pdf(x_n)])


def standardization(x):
    return (x - np.mean(x)) / np.std(x)


def gen_dist_norm(start, end, num):
    from scipy.stats import norm

    x = np.linspace(start, end, num)
    x_s = standardization(x)

    return np.vstack([x, norm.pdf(x_s)])


if __name__ == '__main__':
    # # test_file = 'D:/Localhome/sekim/OneDrive - ZF Friedrichshafen AG/Desktop/NPV concept.xlsx'
    # test_file = 'C:/Users/su79e/Desktop/test.xlsx'
    #
    # wb = xw_load_workbooks(test_file)
    # print(xw_get_selection(wb))
    #
    print(gen_dist_norm(500, 1500, 30))

