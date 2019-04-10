from scipy import stats

# Weibull
def weibull_pdf(x, c, loc, scale):
    return stats.weibull_min.pdf(x, c, loc=loc, scale=scale).flatten()

def weibull_cdf(x, c, loc, scale):
    return stats.weibull_min.cdf(x, c, loc=loc, scale=scale).flatten()

def weibull_sf(x, c, loc, scale):
    return stats.weibull_min.sf(x, c, loc=loc, scale=scale).flatten()


# Pareto
def pareto_pdf(x, b, loc, scale):
    return stats.pareto.pdf([x], b, loc=loc, scale=scale).flatten()

def pareto_cdf(x, b, loc, scale):
    return stats.pareto.cdf([x], b, loc=loc, scale=scale).flatten()

def pareto_sf(x, b, loc, scale):
    return stats.pareto.sf([x], b, loc=loc, scale=scale).flatten()


# Generalized Pareto
def gen_pareto_pdf(x, c, loc, scale):
    return stats.genpareto.pdf(x, c, loc=loc, scale=scale).flatten()

def gen_pareto_cdf(x, c, loc, scale):
    return stats.genpareto.cdf(x, c, loc=loc, scale=scale).flatten()

def gen_pareto_sf(x, c, loc, scale):
    return stats.genpareto.sf(x, c, loc=loc, scale=scale).flatten()


# Exponential
def expon_pdf(x, loc, scale):
    return stats.expon.pdf(x, loc=loc, scale=scale).flatten()

def expon_cdf(x, loc, scale):
    return stats.expon.cdf(x, loc=loc, scale=scale).flatten()

def expon_sf(x, loc, scale):
    return stats.expon.sf(x, loc=loc, scale=scale).flatten()


# non-central Student's t
def student_pdf(x, df, nc, loc, scale):
    return stats.nct.pdf(x, df, nc, loc=loc, scale=scale).flatten()

def student_cdf(x, df, nc, loc, scale):
    return stats.nct.cdf(x, df, nc, loc=loc, scale=scale).flatten()

def student_sf(x, df, nc, loc, scale):
    return stats.nct.sf(x, df, nc, loc=loc, scale=scale).flatten()


# Gamma
def gamma_pdf(x, a, loc, scale):
    return stats.gamma.pdf(x, a, loc=loc, scale=scale).flatten()

def gamma_cdf(x, a, loc, scale):
    return stats.gamma.cdf(x, a, loc=loc, scale=scale).flatten()

def gamma_sf(x, a, loc, scale):
    return stats.gamma.sf(x, a, loc=loc, scale=scale).flatten()


# Log-normal
def lognormal_pdf(x, s, loc, scale):
    return stats.lognorm.pdf(x, s, loc=loc, scale=scale).flatten()

def lognormal_cdf(x, s, loc, scale):
    return stats.lognorm.cdf(x, s, loc=loc, scale=scale).flatten()

def lognormal_sf(x, s, loc, scale):
    return stats.lognorm.sf(x, s, loc=loc, scale=scale).flatten()


# Levy
def levy_pdf(x, loc, scale):
    return stats.levy.pdf(x, loc=loc, scale=scale).flatten()

def levy_cdf(x, loc, scale):
    return stats.levy.cdf(x, loc=loc, scale=scale).flatten()

def levy_sf(x, loc, scale):
    return stats.levy.sf(x, loc=loc, scale=scale).flatten()
