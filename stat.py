from numpy import linspace
from scipy.stats import uniform, norm, expon, beta, bernoulli, binom, poisson


def stat_gen_dist_uniform(start, end, num, loc, scale):
    x, x_step = linspace(start, end, num, retstep=True)

    if not loc:
        loc = start

    if not scale:
        scale = end - start

    return x[1:], uniform.cdf(x[1:], loc, scale) - uniform.cdf(x[:-1], loc, scale)


def stat_gen_dist_normal(start, end, num, loc, scale):
    x, x_step = linspace(start, end, num, retstep=True)

    if not loc:
        loc = x.mean()

    if not scale:
        # norm.cdf(8.293): 1.0
        scale = x.std() / 8.293

    return x[1:], norm.cdf(x[1:], loc=loc, scale=scale) - norm.cdf(x[:-1], loc=loc, scale=scale)


def stat_gen_dist_exponential(start, end, num, loc, scale):
    x, x_step = linspace(start, end, num, retstep=True)

    if not loc:
        loc = start

    if not scale:
        # expon.ppf(1 - (1e-16)): 36.7368005696771
        scale = (end - start) / 38.229 * 2

    return x[1:], expon.cdf(x[1:], loc, scale) - expon.cdf(x[:-1], loc, scale)


def stat_gen_dist_beta(start, end, num, a, b, loc, scale):
    x, x_step = linspace(start, end, num, retstep=True)

    if not loc:
        loc = start

    if not scale:
        scale = end - start

    return x[1:], beta.cdf(x[1:], a, b, loc, scale) - beta.cdf(x[:-1], a, b, loc, scale)


def stat_gen_dist_bernoulli(start, end, p, loc):
    x, x_step = linspace(start, end, 2, retstep=True)

    if not loc:
        loc = 0

    return x, bernoulli(p=p, loc=loc).pmf(k=range(2))


def stat_gen_dist_binom(start, end, num, p, loc):
    x, x_step = linspace(start, end, num, retstep=True)

    if not loc:
        loc = 0

    return x, binom(n=num-1, p=p, loc=loc).pmf(range(num))


def stat_gen_dist_poisson(start, end, num, mu, loc):
    x, x_step = linspace(start, end, num, retstep=True)

    if not loc:
        loc = 0

    return x, poisson(mu=mu, loc=loc).pmf(linspace(0, num, num+1))


if __name__ == '__main__':
    print(stat_gen_dist_uniform(0, 50, 10, 0, 1))

