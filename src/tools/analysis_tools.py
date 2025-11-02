"""
Analysis Tools for AI Data Analyst

Provides statistical analysis, anomaly detection, and data analysis capabilities.
"""

import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Union
import time
from datetime import datetime
# from scipy import stats  # Temporarily disabled for testing
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import warnings
warnings.filterwarnings('ignore')

from utils.config_manager import ConfigManager
from utils.logger import setup_logger, AnalyticsLogger

logger = setup_logger(__name__)
analytics_logger = AnalyticsLogger("analysis")


class AnalysisTools:
    """Statistical analysis and data science tools"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.max_rows = config.get("analytics.max_rows_for_analysis", 1000000)
        self.significance_level = config.get("analytics.statistical_significance", 0.05)
        self.anomaly_contamination = config.get("analytics.anomaly_detection.contamination", 0.1)
    
    async def analyze_data(
        self,
        data_source: str,
        analysis_type: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Perform statistical analysis on data
        
        Args:
            data_source: Data source identifier
            analysis_type: Type of analysis (summary, correlation, regression, clustering)
            columns: Specific columns to analyze
            filters: Data filters to apply
            
        Returns:
            Analysis results
        """
        
        start_time = time.time()
        
        try:
            # Get data from data tools (this would be injected in real implementation)
            # For now, we'll simulate getting data
            data = await self._get_data_from_source(data_source)
            
            if not data:
                return {
                    "success": False,
                    "error": f"No data found for source: {data_source}"
                }
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Apply filters
            if filters:
                df = self._apply_filters(df, filters)
            
            # Select columns
            if columns:
                missing_cols = [col for col in columns if col not in df.columns]
                if missing_cols:
                    return {
                        "success": False,
                        "error": f"Columns not found: {missing_cols}"
                    }
                df = df[columns]
            
            # Check data size
            if len(df) > self.max_rows:
                df = df.sample(n=self.max_rows)
                logger.warning(f"Data truncated to {self.max_rows} rows for analysis")
            
            # Perform analysis based on type
            if analysis_type == "summary":
                result = await self._summary_analysis(df)
            elif analysis_type == "correlation":
                result = await self._correlation_analysis(df)
            elif analysis_type == "regression":
                result = await self._regression_analysis(df, **kwargs)
            elif analysis_type == "clustering":
                result = await self._clustering_analysis(df, **kwargs)
            else:
                return {
                    "success": False,
                    "error": f"Unsupported analysis type: {analysis_type}"
                }
            
            duration = time.time() - start_time
            
            # Add metadata
            result.update({
                "success": True,
                "analysis_type": analysis_type,
                "data_source": data_source,
                "rows_analyzed": len(df),
                "columns_analyzed": list(df.columns),
                "duration_seconds": duration,
                "timestamp": datetime.now().isoformat()
            })
            
            # Log analysis
            analytics_logger.log_analysis(
                analysis_type,
                data_source,
                duration,
                f"Analyzed {len(df)} rows, {len(df.columns)} columns"
            )
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            analytics_logger.log_error("data_analysis", e, {
                "data_source": data_source,
                "analysis_type": analysis_type
            })
            
            return {
                "success": False,
                "error": f"Analysis failed: {str(e)}",
                "duration_seconds": duration
            }
    
    async def detect_anomalies(
        self,
        data_source: str,
        column: str,
        method: str = "isolation_forest",
        threshold: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Detect anomalies in data
        
        Args:
            data_source: Data source identifier
            column: Column to analyze for anomalies
            method: Anomaly detection method
            threshold: Detection threshold
            
        Returns:
            Anomaly detection results
        """
        
        start_time = time.time()
        
        try:
            # Get data
            data = await self._get_data_from_source(data_source)
            
            if not data:
                return {
                    "success": False,
                    "error": f"No data found for source: {data_source}"
                }
            
            df = pd.DataFrame(data)
            
            if column not in df.columns:
                return {
                    "success": False,
                    "error": f"Column not found: {column}"
                }
            
            # Prepare data
            values = df[column].dropna()
            
            if len(values) == 0:
                return {
                    "success": False,
                    "error": f"No valid values in column: {column}"
                }
            
            # Detect anomalies based on method
            if method == "isolation_forest":
                anomalies = self._isolation_forest_anomalies(values, threshold)
            elif method == "z_score":
                anomalies = self._z_score_anomalies(values, threshold or 3.0)
            elif method == "iqr":
                anomalies = self._iqr_anomalies(values, threshold or 1.5)
            else:
                return {
                    "success": False,
                    "error": f"Unsupported anomaly detection method: {method}"
                }
            
            duration = time.time() - start_time
            
            # Prepare results
            result = {
                "success": True,
                "method": method,
                "column": column,
                "data_source": data_source,
                "total_values": len(values),
                "anomalies_detected": len(anomalies["anomaly_indices"]),
                "anomaly_percentage": (len(anomalies["anomaly_indices"]) / len(values)) * 100,
                "anomaly_indices": anomalies["anomaly_indices"],
                "anomaly_values": anomalies["anomaly_values"],
                "statistics": anomalies["statistics"],
                "duration_seconds": duration,
                "timestamp": datetime.now().isoformat()
            }
            
            # Log anomaly detection
            analytics_logger.log_analysis(
                f"anomaly_detection_{method}",
                data_source,
                duration,
                f"Found {len(anomalies['anomaly_indices'])} anomalies in {len(values)} values"
            )
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            analytics_logger.log_error("anomaly_detection", e, {
                "data_source": data_source,
                "column": column,
                "method": method
            })
            
            return {
                "success": False,
                "error": f"Anomaly detection failed: {str(e)}",
                "duration_seconds": duration
            }
    
    async def _summary_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Perform summary statistical analysis"""
        
        summary = {
            "basic_stats": {},
            "data_types": {},
            "missing_values": {},
            "unique_values": {}
        }
        
        for col in df.columns:
            col_data = df[col].dropna()
            
            # Data type
            summary["data_types"][col] = str(df[col].dtype)
            
            # Missing values
            summary["missing_values"][col] = {
                "count": int(df[col].isnull().sum()),
                "percentage": float((df[col].isnull().sum() / len(df)) * 100)
            }
            
            # Unique values
            summary["unique_values"][col] = int(df[col].nunique())
            
            # Basic statistics
            if pd.api.types.is_numeric_dtype(col_data):
                summary["basic_stats"][col] = {
                    "count": int(len(col_data)),
                    "mean": float(col_data.mean()),
                    "median": float(col_data.median()),
                    "std": float(col_data.std()),
                    "min": float(col_data.min()),
                    "max": float(col_data.max()),
                    "q25": float(col_data.quantile(0.25)),
                    "q75": float(col_data.quantile(0.75)),
                    # "skewness": float(stats.skew(col_data)),  # Temporarily disabled
                    # "kurtosis": float(stats.kurtosis(col_data))  # Temporarily disabled
                }
            else:
                summary["basic_stats"][col] = {
                    "count": int(len(col_data)),
                    "unique_count": int(col_data.nunique()),
                    "most_common": str(col_data.mode().iloc[0]) if not col_data.mode().empty else None,
                    "most_common_freq": int(col_data.value_counts().iloc[0]) if len(col_data) > 0 else 0
                }
        
        return {"summary": summary}
    
    async def _correlation_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Perform correlation analysis"""
        
        # Select only numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return {
                "error": "Need at least 2 numeric columns for correlation analysis",
                "numeric_columns": list(numeric_cols)
            }
        
        numeric_df = df[numeric_cols]
        
        # Calculate correlation matrix
        correlation_matrix = numeric_df.corr()
        
        # Find strong correlations
        strong_correlations = []
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                corr_value = correlation_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:  # Strong correlation threshold
                    strong_correlations.append({
                        "variable1": correlation_matrix.columns[i],
                        "variable2": correlation_matrix.columns[j],
                        "correlation": float(corr_value),
                        "strength": "strong" if abs(corr_value) > 0.8 else "moderate"
                    })
        
        return {
            "correlation_matrix": correlation_matrix.to_dict(),
            "strong_correlations": strong_correlations,
            "numeric_columns": list(numeric_cols)
        }
    
    async def _regression_analysis(self, df: pd.DataFrame, target_column: str = None, **kwargs) -> Dict[str, Any]:
        """Perform regression analysis"""
        
        if not target_column:
            # Use the last numeric column as target
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                return {"error": "No numeric columns found for regression"}
            target_column = numeric_cols[-1]
        
        if target_column not in df.columns:
            return {"error": f"Target column not found: {target_column}"}
        
        # Prepare features and target
        numeric_df = df.select_dtypes(include=[np.number])
        feature_cols = [col for col in numeric_df.columns if col != target_column]
        
        if len(feature_cols) == 0:
            return {"error": "No feature columns available for regression"}
        
        X = numeric_df[feature_cols].fillna(0)
        y = numeric_df[target_column].fillna(0)
        
        # Perform linear regression
        model = LinearRegression()
        model.fit(X, y)
        
        # Predictions
        y_pred = model.predict(X)
        r2 = r2_score(y, y_pred)
        
        # Feature importance (coefficients)
        feature_importance = []
        for i, col in enumerate(feature_cols):
            feature_importance.append({
                "feature": col,
                "coefficient": float(model.coef_[i]),
                "abs_coefficient": float(abs(model.coef_[i]))
            })
        
        feature_importance.sort(key=lambda x: x["abs_coefficient"], reverse=True)
        
        return {
            "target_column": target_column,
            "feature_columns": feature_cols,
            "r_squared": float(r2),
            "intercept": float(model.intercept_),
            "feature_importance": feature_importance,
            "model_performance": "good" if r2 > 0.7 else "moderate" if r2 > 0.5 else "poor"
        }
    
    async def _clustering_analysis(self, df: pd.DataFrame, n_clusters: int = None, **kwargs) -> Dict[str, Any]:
        """Perform clustering analysis"""
        
        # Select numeric columns
        numeric_df = df.select_dtypes(include=[np.number]).fillna(0)
        
        if len(numeric_df.columns) == 0:
            return {"error": "No numeric columns found for clustering"}
        
        # Determine optimal number of clusters if not provided
        if not n_clusters:
            n_clusters = min(5, max(2, len(numeric_df) // 20))  # Heuristic
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(numeric_df)
        
        # Perform K-means clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        cluster_labels = kmeans.fit_predict(X_scaled)
        
        # Analyze clusters
        cluster_analysis = {}
        for i in range(n_clusters):
            cluster_mask = cluster_labels == i
            cluster_data = numeric_df[cluster_mask]
            
            cluster_analysis[f"cluster_{i}"] = {
                "size": int(cluster_mask.sum()),
                "percentage": float((cluster_mask.sum() / len(numeric_df)) * 100),
                "centroid": {
                    col: float(cluster_data[col].mean())
                    for col in numeric_df.columns
                }
            }
        
        return {
            "n_clusters": n_clusters,
            "cluster_labels": cluster_labels.tolist(),
            "cluster_analysis": cluster_analysis,
            "features_used": list(numeric_df.columns),
            "inertia": float(kmeans.inertia_)
        }
    
    def _isolation_forest_anomalies(self, values: pd.Series, contamination: Optional[float] = None) -> Dict[str, Any]:
        """Detect anomalies using Isolation Forest"""
        
        contamination = contamination or self.anomaly_contamination
        
        # Reshape for sklearn
        X = values.values.reshape(-1, 1)
        
        # Fit Isolation Forest
        iso_forest = IsolationForest(contamination=contamination, random_state=42)
        anomaly_labels = iso_forest.fit_predict(X)
        
        # Get anomaly indices and values
        anomaly_mask = anomaly_labels == -1
        anomaly_indices = values.index[anomaly_mask].tolist()
        anomaly_values = values[anomaly_mask].tolist()
        
        return {
            "anomaly_indices": anomaly_indices,
            "anomaly_values": anomaly_values,
            "statistics": {
                "contamination_rate": contamination,
                "mean": float(values.mean()),
                "std": float(values.std()),
                "min": float(values.min()),
                "max": float(values.max())
            }
        }
    
    def _z_score_anomalies(self, values: pd.Series, threshold: float = 3.0) -> Dict[str, Any]:
        """Detect anomalies using Z-score"""
        
        # z_scores = np.abs(stats.zscore(values))  # Temporarily disabled
        # Use basic numpy implementation for z-score calculation
        z_scores = np.abs((values - values.mean()) / values.std())
        anomaly_mask = z_scores > threshold
        
        anomaly_indices = values.index[anomaly_mask].tolist()
        anomaly_values = values[anomaly_mask].tolist()
        
        return {
            "anomaly_indices": anomaly_indices,
            "anomaly_values": anomaly_values,
            "statistics": {
                "threshold": threshold,
                "mean": float(values.mean()),
                "std": float(values.std()),
                "z_scores": z_scores.tolist()
            }
        }
    
    def _iqr_anomalies(self, values: pd.Series, multiplier: float = 1.5) -> Dict[str, Any]:
        """Detect anomalies using Interquartile Range (IQR)"""
        
        Q1 = values.quantile(0.25)
        Q3 = values.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        anomaly_mask = (values < lower_bound) | (values > upper_bound)
        
        anomaly_indices = values.index[anomaly_mask].tolist()
        anomaly_values = values[anomaly_mask].tolist()
        
        return {
            "anomaly_indices": anomaly_indices,
            "anomaly_values": anomaly_values,
            "statistics": {
                "Q1": float(Q1),
                "Q3": float(Q3),
                "IQR": float(IQR),
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound),
                "multiplier": multiplier
            }
        }
    
    def _apply_filters(self, df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """Apply filters to DataFrame"""
        
        filtered_df = df.copy()
        
        for column, filter_value in filters.items():
            if column not in filtered_df.columns:
                continue
            
            if isinstance(filter_value, dict):
                # Range filter
                if "min" in filter_value:
                    filtered_df = filtered_df[filtered_df[column] >= filter_value["min"]]
                if "max" in filter_value:
                    filtered_df = filtered_df[filtered_df[column] <= filter_value["max"]]
            elif isinstance(filter_value, list):
                # Value list filter
                filtered_df = filtered_df[filtered_df[column].isin(filter_value)]
            else:
                # Exact value filter
                filtered_df = filtered_df[filtered_df[column] == filter_value]
        
        return filtered_df
    
    async def _get_data_from_source(self, data_source: str) -> Optional[List[Dict[str, Any]]]:
        """Get data from data source (placeholder - would be injected in real implementation)"""
        
        # This is a placeholder - in the real implementation, this would
        # get data from the DataTools instance
        
        # For demo purposes, return sample data
        if "sales" in data_source.lower():
            return [
                {"date": "2024-01-01", "amount": 1000, "region": "North", "customer": "A"},
                {"date": "2024-01-02", "amount": 1500, "region": "South", "customer": "B"},
                {"date": "2024-01-03", "amount": 800, "region": "North", "customer": "C"},
                {"date": "2024-01-04", "amount": 2000, "region": "East", "customer": "D"},
                {"date": "2024-01-05", "amount": 1200, "region": "West", "customer": "E"}
            ]
        
        return None 