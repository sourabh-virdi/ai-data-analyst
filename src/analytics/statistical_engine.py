"""
Statistical Analysis Engine

Provides core statistical analysis functions that can be used
across different tools and components.
"""

import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from scipy import stats
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')


class StatisticalEngine:
    """Core statistical analysis engine"""
    
    def __init__(self):
        self.scaler = StandardScaler()
    
    def descriptive_statistics(self, data: pd.Series) -> Dict[str, float]:
        """Calculate descriptive statistics for a series"""
        
        if len(data) == 0:
            return {}
        
        # Remove NaN values
        clean_data = data.dropna()
        
        if len(clean_data) == 0:
            return {"count": 0}
        
        stats_dict = {
            "count": len(clean_data),
            "mean": float(clean_data.mean()),
            "median": float(clean_data.median()),
            "std": float(clean_data.std()),
            "var": float(clean_data.var()),
            "min": float(clean_data.min()),
            "max": float(clean_data.max()),
            "q25": float(clean_data.quantile(0.25)),
            "q75": float(clean_data.quantile(0.75)),
            "skewness": float(stats.skew(clean_data)),
            "kurtosis": float(stats.kurtosis(clean_data)),
            "range": float(clean_data.max() - clean_data.min()),
            "iqr": float(clean_data.quantile(0.75) - clean_data.quantile(0.25))
        }
        
        return stats_dict
    
    def correlation_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Perform correlation analysis on numeric columns"""
        
        # Select only numeric columns
        numeric_df = df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            return {"error": "Need at least 2 numeric columns for correlation analysis"}
        
        # Calculate correlation matrices
        pearson_corr = numeric_df.corr(method='pearson')
        spearman_corr = numeric_df.corr(method='spearman')
        
        # Find significant correlations
        significant_correlations = []
        
        for i in range(len(pearson_corr.columns)):
            for j in range(i+1, len(pearson_corr.columns)):
                col1, col2 = pearson_corr.columns[i], pearson_corr.columns[j]
                pearson_val = pearson_corr.iloc[i, j]
                spearman_val = spearman_corr.iloc[i, j]
                
                if abs(pearson_val) > 0.3:  # Threshold for significant correlation
                    significant_correlations.append({
                        "variable1": col1,
                        "variable2": col2,
                        "pearson_correlation": float(pearson_val),
                        "spearman_correlation": float(spearman_val),
                        "strength": self._correlation_strength(abs(pearson_val))
                    })
        
        return {
            "pearson_matrix": pearson_corr.to_dict(),
            "spearman_matrix": spearman_corr.to_dict(),
            "significant_correlations": significant_correlations,
            "correlation_count": len(significant_correlations)
        }
    
    def normality_test(self, data: pd.Series, alpha: float = 0.05) -> Dict[str, Any]:
        """Test for normality using Shapiro-Wilk test"""
        
        clean_data = data.dropna()
        
        if len(clean_data) < 3:
            return {"error": "Need at least 3 observations for normality test"}
        
        # Use Shapiro-Wilk for small samples, Anderson-Darling for larger
        if len(clean_data) <= 5000:
            statistic, p_value = stats.shapiro(clean_data)
            test_name = "Shapiro-Wilk"
        else:
            # For large samples, use Anderson-Darling
            result = stats.anderson(clean_data, dist='norm')
            statistic = result.statistic
            p_value = 0.05 if statistic > result.critical_values[2] else 0.1  # Approximation
            test_name = "Anderson-Darling"
        
        is_normal = p_value > alpha
        
        return {
            "test_name": test_name,
            "statistic": float(statistic),
            "p_value": float(p_value),
            "is_normal": is_normal,
            "alpha": alpha,
            "interpretation": "Data appears normally distributed" if is_normal else "Data does not appear normally distributed"
        }
    
    def outlier_detection_iqr(self, data: pd.Series, multiplier: float = 1.5) -> Dict[str, Any]:
        """Detect outliers using Interquartile Range method"""
        
        clean_data = data.dropna()
        
        if len(clean_data) == 0:
            return {"error": "No valid data for outlier detection"}
        
        Q1 = clean_data.quantile(0.25)
        Q3 = clean_data.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        outliers_mask = (clean_data < lower_bound) | (clean_data > upper_bound)
        outliers = clean_data[outliers_mask]
        
        return {
            "method": "IQR",
            "Q1": float(Q1),
            "Q3": float(Q3),
            "IQR": float(IQR),
            "lower_bound": float(lower_bound),
            "upper_bound": float(upper_bound),
            "outlier_count": len(outliers),
            "outlier_percentage": (len(outliers) / len(clean_data)) * 100,
            "outlier_values": outliers.tolist(),
            "outlier_indices": outliers.index.tolist()
        }
    
    def trend_analysis(self, data: pd.Series, dates: pd.Series = None) -> Dict[str, Any]:
        """Analyze trends in time series data"""
        
        if dates is not None:
            # Ensure dates are datetime
            if not pd.api.types.is_datetime64_any_dtype(dates):
                try:
                    dates = pd.to_datetime(dates)
                except:
                    dates = None
        
        clean_data = data.dropna()
        
        if len(clean_data) < 3:
            return {"error": "Need at least 3 observations for trend analysis"}
        
        # Simple linear trend
        x = np.arange(len(clean_data))
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, clean_data)
        
        # Mann-Kendall trend test
        mk_result = self._mann_kendall_test(clean_data.values)
        
        # Determine trend direction
        if p_value < 0.05:
            if slope > 0:
                trend_direction = "increasing"
            else:
                trend_direction = "decreasing"
        else:
            trend_direction = "no significant trend"
        
        result = {
            "linear_trend": {
                "slope": float(slope),
                "intercept": float(intercept),
                "r_squared": float(r_value ** 2),
                "p_value": float(p_value),
                "standard_error": float(std_err)
            },
            "mann_kendall": mk_result,
            "trend_direction": trend_direction,
            "trend_strength": abs(float(r_value)),
            "data_points": len(clean_data)
        }
        
        return result
    
    def _mann_kendall_test(self, data: np.ndarray) -> Dict[str, Any]:
        """Perform Mann-Kendall trend test"""
        
        n = len(data)
        
        # Calculate S statistic
        S = 0
        for i in range(n-1):
            for j in range(i+1, n):
                if data[j] > data[i]:
                    S += 1
                elif data[j] < data[i]:
                    S -= 1
        
        # Calculate variance
        var_S = n * (n - 1) * (2 * n + 5) / 18
        
        # Calculate Z statistic
        if S > 0:
            Z = (S - 1) / np.sqrt(var_S)
        elif S < 0:
            Z = (S + 1) / np.sqrt(var_S)
        else:
            Z = 0
        
        # Calculate p-value (two-tailed test)
        p_value = 2 * (1 - stats.norm.cdf(abs(Z)))
        
        # Determine trend
        alpha = 0.05
        if p_value < alpha:
            if S > 0:
                trend = "increasing"
            else:
                trend = "decreasing"
        else:
            trend = "no trend"
        
        return {
            "S_statistic": int(S),
            "Z_statistic": float(Z),
            "p_value": float(p_value),
            "trend": trend,
            "alpha": alpha
        }
    
    def _correlation_strength(self, correlation: float) -> str:
        """Interpret correlation strength"""
        
        if correlation >= 0.8:
            return "very strong"
        elif correlation >= 0.6:
            return "strong"
        elif correlation >= 0.4:
            return "moderate"
        elif correlation >= 0.2:
            return "weak"
        else:
            return "very weak"
    
    def hypothesis_test(self, sample1: pd.Series, sample2: pd.Series = None, test_type: str = "ttest") -> Dict[str, Any]:
        """Perform hypothesis tests"""
        
        sample1_clean = sample1.dropna()
        
        if test_type == "ttest_1samp":
            # One-sample t-test (test if mean differs from 0)
            statistic, p_value = stats.ttest_1samp(sample1_clean, 0)
            
            return {
                "test_type": "One-sample t-test",
                "statistic": float(statistic),
                "p_value": float(p_value),
                "sample_mean": float(sample1_clean.mean()),
                "sample_size": len(sample1_clean)
            }
        
        elif test_type == "ttest_ind" and sample2 is not None:
            # Independent two-sample t-test
            sample2_clean = sample2.dropna()
            statistic, p_value = stats.ttest_ind(sample1_clean, sample2_clean)
            
            return {
                "test_type": "Independent two-sample t-test",
                "statistic": float(statistic),
                "p_value": float(p_value),
                "sample1_mean": float(sample1_clean.mean()),
                "sample2_mean": float(sample2_clean.mean()),
                "sample1_size": len(sample1_clean),
                "sample2_size": len(sample2_clean)
            }
        
        else:
            return {"error": f"Unsupported test type: {test_type}"}
    
    def confidence_interval(self, data: pd.Series, confidence: float = 0.95) -> Dict[str, Any]:
        """Calculate confidence interval for the mean"""
        
        clean_data = data.dropna()
        
        if len(clean_data) < 2:
            return {"error": "Need at least 2 observations for confidence interval"}
        
        mean = clean_data.mean()
        sem = stats.sem(clean_data)  # Standard error of mean
        
        # Use t-distribution for small samples, normal for large samples
        if len(clean_data) < 30:
            # t-distribution
            df = len(clean_data) - 1
            t_critical = stats.t.ppf((1 + confidence) / 2, df)
            margin_error = t_critical * sem
        else:
            # Normal distribution
            z_critical = stats.norm.ppf((1 + confidence) / 2)
            margin_error = z_critical * sem
        
        ci_lower = mean - margin_error
        ci_upper = mean + margin_error
        
        return {
            "mean": float(mean),
            "confidence_level": confidence,
            "margin_of_error": float(margin_error),
            "confidence_interval": [float(ci_lower), float(ci_upper)],
            "sample_size": len(clean_data),
            "standard_error": float(sem)
        } 