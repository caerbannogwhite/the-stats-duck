#pragma once

#include <cmath>
#include <limits>
#include <stdexcept>

namespace stats_duck {

//! Log of the Beta function: log(B(a, b)) = lgamma(a) + lgamma(b) - lgamma(a+b)
inline double LogBeta(double a, double b) {
	return std::lgamma(a) + std::lgamma(b) - std::lgamma(a + b);
}

//! Regularized incomplete beta function I_x(a, b) using the continued fraction expansion.
//! Uses the Lentz algorithm (modified) for numerical stability.
inline double BetaIncomplete(double a, double b, double x) {
	if (x < 0.0 || x > 1.0) {
		throw std::invalid_argument("x must be in [0, 1]");
	}
	if (x == 0.0) {
		return 0.0;
	}
	if (x == 1.0) {
		return 1.0;
	}

	// Use the symmetry relation when x > (a+1)/(a+b+2) for better convergence
	if (x > (a + 1.0) / (a + b + 2.0)) {
		return 1.0 - BetaIncomplete(b, a, 1.0 - x);
	}

	// Prefactor: x^a * (1-x)^b / (a * B(a,b))
	double log_prefix = a * std::log(x) + b * std::log(1.0 - x) - LogBeta(a, b) - std::log(a);

	// Lentz's continued fraction algorithm
	const double eps = std::numeric_limits<double>::epsilon();
	const double tiny = 1e-30;
	const int max_iter = 200;

	// The continued fraction for I_x(a,b) is:
	//   I_x(a,b) = prefix * 1/(1+ d1/(1+ d2/(1+ ...)))
	// where d_m are the continued fraction coefficients.
	double f = 1.0;
	double c = 1.0;
	double d = 1.0 - (a + b) * x / (a + 1.0);
	if (std::abs(d) < tiny) {
		d = tiny;
	}
	d = 1.0 / d;
	f = d;

	for (int m = 1; m <= max_iter; m++) {
		// Even step: d_{2m}
		double m_d = static_cast<double>(m);
		double numerator = m_d * (b - m_d) * x / ((a + 2.0 * m_d - 1.0) * (a + 2.0 * m_d));

		d = 1.0 + numerator * d;
		if (std::abs(d) < tiny) {
			d = tiny;
		}
		c = 1.0 + numerator / c;
		if (std::abs(c) < tiny) {
			c = tiny;
		}
		d = 1.0 / d;
		f *= c * d;

		// Odd step: d_{2m+1}
		numerator = -((a + m_d) * (a + b + m_d) * x) / ((a + 2.0 * m_d) * (a + 2.0 * m_d + 1.0));

		d = 1.0 + numerator * d;
		if (std::abs(d) < tiny) {
			d = tiny;
		}
		c = 1.0 + numerator / c;
		if (std::abs(c) < tiny) {
			c = tiny;
		}
		d = 1.0 / d;
		double delta = c * d;
		f *= delta;

		if (std::abs(delta - 1.0) < eps) {
			break;
		}
	}

	return std::exp(log_prefix) * f;
}

//! CDF of Student's t-distribution with df degrees of freedom
inline double StudentTCDF(double t, double df) {
	double x = df / (df + t * t);
	double ibeta = BetaIncomplete(df / 2.0, 0.5, x);

	if (t >= 0.0) {
		return 1.0 - 0.5 * ibeta;
	} else {
		return 0.5 * ibeta;
	}
}

//! Quantile (inverse CDF) of Student's t-distribution via bisection
inline double StudentTQuantile(double p, double df) {
	if (p <= 0.0 || p >= 1.0) {
		if (p == 0.0) {
			return -std::numeric_limits<double>::infinity();
		}
		if (p == 1.0) {
			return std::numeric_limits<double>::infinity();
		}
		throw std::invalid_argument("p must be in (0, 1)");
	}

	// Bisection search
	double lo = -1000.0;
	double hi = 1000.0;

	// Widen bounds if needed
	while (StudentTCDF(lo, df) > p) {
		lo *= 2.0;
	}
	while (StudentTCDF(hi, df) < p) {
		hi *= 2.0;
	}

	// Bisect to machine precision
	for (int i = 0; i < 100; i++) {
		double mid = 0.5 * (lo + hi);
		if (StudentTCDF(mid, df) < p) {
			lo = mid;
		} else {
			hi = mid;
		}
	}

	return 0.5 * (lo + hi);
}

} // namespace stats_duck
