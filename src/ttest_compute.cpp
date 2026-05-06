#include "ttest_compute.hpp"

#include <cmath>
#include <numeric>
#include <stdexcept>

#include "distributions.hpp"

namespace stats_duck {

static double Mean(const std::vector<double> &v) {
	return std::accumulate(v.begin(), v.end(), 0.0) / static_cast<double>(v.size());
}

static double Variance(const std::vector<double> &v, double mean) {
	double sum_sq = 0.0;
	for (auto x : v) {
		double diff = x - mean;
		sum_sq += diff * diff;
	}
	return sum_sq / static_cast<double>(v.size() - 1);
}

static double ComputePValue(double t_stat, double df, const std::string &alternative) {
	if (alternative == "two-sided") {
		return 2.0 * (1.0 - StudentTCDF(std::abs(t_stat), df));
	} else if (alternative == "less") {
		return StudentTCDF(t_stat, df);
	} else { // "greater"
		return 1.0 - StudentTCDF(t_stat, df);
	}
}

static void ComputeCI(double mean_diff, double se, double df, double alpha, const std::string &alternative,
                       double &ci_lower, double &ci_upper) {
	if (alternative == "two-sided") {
		double t_crit = StudentTQuantile(1.0 - alpha / 2.0, df);
		ci_lower = mean_diff - t_crit * se;
		ci_upper = mean_diff + t_crit * se;
	} else if (alternative == "less") {
		double t_crit = StudentTQuantile(1.0 - alpha, df);
		ci_lower = -std::numeric_limits<double>::infinity();
		ci_upper = mean_diff + t_crit * se;
	} else { // "greater"
		double t_crit = StudentTQuantile(1.0 - alpha, df);
		ci_lower = mean_diff - t_crit * se;
		ci_upper = std::numeric_limits<double>::infinity();
	}
}

TTestResult ComputeTTest1Samp(const std::vector<double> &sample, double mu, double alpha,
                              const std::string &alternative) {
	auto n = sample.size();
	if (n < 2) {
		throw std::invalid_argument("One-sample t-test requires at least 2 observations");
	}

	double mean = Mean(sample);
	double var = Variance(sample, mean);
	double sd = std::sqrt(var);
	double se = sd / std::sqrt(static_cast<double>(n));
	double df = static_cast<double>(n - 1);

	double t_stat = (mean - mu) / se;
	double mean_diff = mean - mu;
	double p_value = ComputePValue(t_stat, df, alternative);
	double cohens_d = mean_diff / sd;

	double ci_lower, ci_upper;
	ComputeCI(mean_diff, se, df, alpha, alternative, ci_lower, ci_upper);

	return TTestResult {"One-sample", t_stat, df, p_value, alternative, mean_diff, ci_lower, ci_upper, cohens_d};
}

TTestResult ComputeTTest2Samp(const std::vector<double> &sample1, const std::vector<double> &sample2, bool equal_var,
                              double alpha, const std::string &alternative) {
	auto n1 = sample1.size();
	auto n2 = sample2.size();
	if (n1 < 2 || n2 < 2) {
		throw std::invalid_argument("Two-sample t-test requires at least 2 observations per group");
	}

	double mean1 = Mean(sample1);
	double mean2 = Mean(sample2);
	double var1 = Variance(sample1, mean1);
	double var2 = Variance(sample2, mean2);
	double dn1 = static_cast<double>(n1);
	double dn2 = static_cast<double>(n2);

	double mean_diff = mean1 - mean2;
	double t_stat, df, se;
	std::string test_type;

	if (equal_var) {
		// Student's pooled t-test
		double sp2 = ((dn1 - 1.0) * var1 + (dn2 - 1.0) * var2) / (dn1 + dn2 - 2.0);
		se = std::sqrt(sp2 * (1.0 / dn1 + 1.0 / dn2));
		df = dn1 + dn2 - 2.0;
		test_type = "Student Two-sample";
	} else {
		// Welch's t-test
		double v1n = var1 / dn1;
		double v2n = var2 / dn2;
		se = std::sqrt(v1n + v2n);
		double num = (v1n + v2n) * (v1n + v2n);
		double denom = (v1n * v1n) / (dn1 - 1.0) + (v2n * v2n) / (dn2 - 1.0);
		df = num / denom;
		test_type = "Welch Two-sample";
	}

	t_stat = mean_diff / se;
	double p_value = ComputePValue(t_stat, df, alternative);

	// Pooled SD for Cohen's d
	double sp = std::sqrt(((dn1 - 1.0) * var1 + (dn2 - 1.0) * var2) / (dn1 + dn2 - 2.0));
	double cohens_d = mean_diff / sp;

	double ci_lower, ci_upper;
	ComputeCI(mean_diff, se, df, alpha, alternative, ci_lower, ci_upper);

	return TTestResult {test_type, t_stat, df, p_value, alternative, mean_diff, ci_lower, ci_upper, cohens_d};
}

TTestResult ComputeTTestPaired(const std::vector<double> &sample1, const std::vector<double> &sample2, double alpha,
                               const std::string &alternative) {
	if (sample1.size() != sample2.size()) {
		throw std::invalid_argument("Paired t-test requires equal-length samples");
	}

	// Compute differences and delegate to one-sample t-test
	std::vector<double> diffs(sample1.size());
	for (size_t i = 0; i < sample1.size(); i++) {
		diffs[i] = sample1[i] - sample2[i];
	}

	TTestResult result = ComputeTTest1Samp(diffs, 0.0, alpha, alternative);
	result.test_type = "Paired";
	return result;
}

} // namespace stats_duck
