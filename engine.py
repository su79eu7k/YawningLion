from math import ceil
import numpy as np
from scipy.stats import uniform, norm, expon, beta, bernoulli
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


def stat_gen_dist_uniform(start, end, num, loc, scale):
    x, x_step = np.linspace(start, end, num, retstep=True)

    if not loc:
        loc = start

    if not scale:
        scale = end - start

    return x[1:], uniform.cdf(x[1:], loc, scale) - uniform.cdf(x[:-1], loc, scale)


def stat_gen_dist_normal(start, end, num, loc, scale):
    x, x_step = np.linspace(start, end, num, retstep=True)

    if not loc:
        loc = x.mean()

    if not scale:
        # norm.cdf(8.293): 1.0
        scale = x.std() / 8.293

    return x[1:], norm.cdf(x[1:], loc=loc, scale=scale) - norm.cdf(x[:-1], loc=loc, scale=scale)


def stat_gen_dist_exponential(start, end, num, loc, scale):
    x, x_step = np.linspace(start, end, num, retstep=True)

    if not loc:
        loc = start

    if not scale:
        # expon.ppf(1 - (1e-16)): 36.7368005696771
        scale = (end - start) / 38.229 * 2

    return x[1:], expon.cdf(x[1:], loc, scale) - expon.cdf(x[:-1], loc, scale)


def stat_gen_dist_beta(start, end, num, a, b, loc, scale):
    x, x_step = np.linspace(start, end, num, retstep=True)

    if not loc:
        loc = start

    if not scale:
        scale = end - start

    return x[1:], beta.cdf(x[1:], a, b, loc, scale) - beta.cdf(x[:-1], a, b, loc, scale)


def stat_gen_dist_bernoulli(start, end, p, loc):
    x = np.array([start, end])

    return x, bernoulli.pmf(x, p, loc)


if __name__ == '__main__':
    print(stat_gen_dist_uniform(0, 50, 10, 0, 1))

