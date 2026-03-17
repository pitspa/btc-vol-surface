import numpy as np


def static_replication_nth_moment(n, K_array, calls_array, puts_array, F, S0, T):
    # Bakshi, Kapadia & Madan (2003) static replication of the n-th
    # centralised risk-neutral moment of the simple return.
    #
    # n=2: variance contract (SVIX^2 when annualised)
    # n=3: cubic contract (skewness contribution)
    # n=4: quartic contract (kurtosis contribution)
    #
    # All prices and strikes must be in USD.
    # Rf = F/S0 is the gross risk-free return (forward/spot).
    # Returns the annualised (1/T) moment contract value.

    Rf = F / S0

    # Weight: (K/S0 - Rf)^(n-2)
    # For n=2 this is 1, for n=3 linear, for n=4 quadratic
    weights = (K_array / S0 - Rf) ** (n - 2)

    # OTM selection: puts where K <= F, calls where K > F
    otm_prices = np.where(K_array <= F, puts_array, calls_array)

    # Weighted integrand
    integrand = weights * otm_prices

    # Trapezoidal integration over strikes
    integral = np.trapz(integrand, K_array)

    # Prefactor: n(n-1) * Rf / S0^2, annualised by 1/T
    prefactor = n * (n - 1) * Rf / (S0 ** 2 * T)

    return prefactor * integral
