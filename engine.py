from math import ceil
import numpy as np
from numpy import inf
from scipy.stats import uniform, norm, expon, beta
import xlwings as xw


def util_build_chunks(lst, size):
    return list(map(
        lambda x: lst[x * size:x * size + size], list(range(0, ceil(len(lst) / size)))))


def xw_load_workbooks(filepath):
    return xw.Book(filepath)


def xw_activate_workbook(func):
    def wrapper(workbook_to_activate, *args, **kwargs):
        workbook_to_activate.activate()
        return func(workbook_to_activate, *args, **kwargs)
    return wrapper


@xw_activate_workbook
def xw_get_selection(workbook):
    return workbook.selection.get_address(False, False, True, False)


@xw_activate_workbook
def xw_select_with_focus(workbook, address_sheet, address_cell):
    workbook.sheets[address_sheet].range(address_cell).select()
    workbook.activate(steal_focus=True)

    return True


def stat_min_max_norm(x):
    return (x - np.min(x)) / (np.max(x) - np.min(x))


def stat_gen_dist_uniform(start, end, num, loc, scale):
    x, x_step = np.linspace(start, end, num, retstep=True)

    if not loc:
        loc = x[0] - x_step

    if not scale:
        scale = x[-1] - x[0]

    return x, uniform.cdf(x, loc, scale) - uniform.cdf(x - x_step, loc, scale)


def stat_gen_dist_normal(start, end, num, loc, scale):
    x, x_step = np.linspace(start, end, num, retstep=True)

    if not loc:
        loc = x.mean()

    if not scale:
        scale = x.std()

    return x, norm.cdf(x, loc=loc, scale=scale) - norm.cdf(x - x_step, loc=loc, scale=scale)


def stat_gen_dist_exponential(start, end, num, loc, scale):
    x, x_step = np.linspace(start, end, num, retstep=True)

    if not loc:
        loc = x[0] - x_step

    if not scale:
        _gap = str(x[-1] - x[0])
        scale = float("1" + "".zfill(len(_gap)))

    return x, expon.cdf(x, loc, scale) - expon.cdf(x - x_step, loc, scale)


def stat_gen_dist_beta(start, end, num, a, b, loc, scale):
    x, x_step = np.linspace(start, end, num, retstep=True)

    if not loc:
        loc = x[0] - x_step

    if not scale:
        scale = x[-1] - x[0]

    return x, beta.cdf(x, a, b, loc, scale) - beta.cdf(x - x_step, a, b, loc, scale)


if __name__ == '__main__':
    # # test_file = 'D:/Localhome/sekim/OneDrive - ZF Friedrichshafen AG/Desktop/NPV concept.xlsx'
    # test_file = 'C:/Users/su79e/Desktop/test.xlsx'
    #
    # wb = xw_load_workbooks(test_file)
    # print(xw_get_selection(wb))
    #
    print(stat_gen_dist_uniform(500, 1500, 30).shape)

