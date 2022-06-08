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
    x = np.linspace(start, end, num)

    if not loc:
        loc = x[0]

    if not scale:
        scale = x[-1] - x[0]

    return x, uniform.pdf(x, loc, scale)


def stat_gen_dist_normal(start, end, num, loc, scale):
    x = np.linspace(start, end, num)

    if not loc:
        loc = x.mean()

    if not scale:
        scale = x.std()

    return x, norm.pdf(x, loc=loc, scale=scale)


def stat_gen_dist_exponential(start, end, num, loc, scale):
    x = np.linspace(start, end, num)

    if not loc:
        loc = x[0]

    if not scale:
        scale = 1

    return x, expon.pdf(x, loc, scale)


def stat_gen_dist_beta(start, end, num, a, b, loc=0, scale=1):
    x = np.linspace(start, end, num)
    x_n = stat_min_max_norm(x)

    if a == 1 and b == 1:
        return x, uniform.pdf(x_n, loc, scale)
    else:
        _ret = beta.pdf(x_n, a, b, loc, scale)
        _idx_inf = np.isinf(_ret)
        if _idx_inf.any():
            return x[_idx_inf], np.full(x[_idx_inf].shape, 1)
        else:
            return x, _ret


if __name__ == '__main__':
    # # test_file = 'D:/Localhome/sekim/OneDrive - ZF Friedrichshafen AG/Desktop/NPV concept.xlsx'
    # test_file = 'C:/Users/su79e/Desktop/test.xlsx'
    #
    # wb = xw_load_workbooks(test_file)
    # print(xw_get_selection(wb))
    #
    print(stat_gen_dist_uniform(500, 1500, 30).shape)

