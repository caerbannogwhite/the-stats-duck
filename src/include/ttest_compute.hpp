#pragma once

#include <string>
#include <vector>

namespace stats_duck {

struct TTestResult {
	std::string test_type;
	double t_statistic;
	double df;
	double p_value;
	std::string alternative;
	double mean_diff;
	double ci_lower;
	double ci_upper;
	double cohens_d;
};

//! One-sample t-test: test whether the mean of sample equals mu
TTestResult ComputeTTest1Samp(const std::vector<double> &sample, double mu, double alpha,
                              const std::string &alternative);

//! Two-sample t-test: Welch's (default) or Student's pooled
TTestResult ComputeTTest2Samp(const std::vector<double> &sample1, const std::vector<double> &sample2, bool equal_var,
                              double alpha, const std::string &alternative);

//! Paired t-test: test whether the mean difference between paired observations is zero
TTestResult ComputeTTestPaired(const std::vector<double> &sample1, const std::vector<double> &sample2, double alpha,
                               const std::string &alternative);

} // namespace stats_duck
