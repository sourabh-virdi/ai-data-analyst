"""
Visualization Tools for AI Data Analyst

Provides chart generation and dashboard creation capabilities using
Matplotlib, Plotly, and other visualization libraries.
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Union
import time
from datetime import datetime
from pathlib import Path
import base64
import io

from utils.config_manager import ConfigManager
from utils.logger import setup_logger, AnalyticsLogger

logger = setup_logger(__name__)
analytics_logger = AnalyticsLogger("visualization")


class VisualizationTools:
    """Chart generation and dashboard creation tools"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.default_theme = config.get("visualization.default_theme", "plotly_white")
        self.export_formats = config.get("visualization.export_formats", ["png", "html", "svg"])
        self.default_width = config.get("visualization.default_width", 800)
        self.default_height = config.get("visualization.default_height", 600)
        self.color_palette = config.get("visualization.color_palette", [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"
        ])
        
        # Create output directory
        self.output_dir = Path("outputs/charts")
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    async def create_chart(
        self,
        data_source: str,
        chart_type: str,
        x_column: Optional[str] = None,
        y_column: Optional[str] = None,
        group_by: Optional[str] = None,
        title: Optional[str] = None,
        export_format: str = "png",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create various types of charts
        
        Args:
            data_source: Data source identifier
            chart_type: Type of chart to create
            x_column: X-axis column
            y_column: Y-axis column
            group_by: Column to group by
            title: Chart title
            export_format: Export format
            
        Returns:
            Chart creation results with file path
        """
        
        start_time = time.time()
        
        try:
            # Get data from data source
            data = await self._get_data_from_source(data_source)
            
            if not data:
                return {
                    "success": False,
                    "error": f"No data found for source: {data_source}"
                }
            
            df = pd.DataFrame(data)
            
            # Validate columns
            if x_column and x_column not in df.columns:
                return {"success": False, "error": f"X column not found: {x_column}"}
            if y_column and y_column not in df.columns:
                return {"success": False, "error": f"Y column not found: {y_column}"}
            if group_by and group_by not in df.columns:
                return {"success": False, "error": f"Group by column not found: {group_by}"}
            
            # Create chart based on type
            if chart_type == "bar":
                chart_result = await self._create_bar_chart(df, x_column, y_column, group_by, title, **kwargs)
            elif chart_type == "line":
                chart_result = await self._create_line_chart(df, x_column, y_column, group_by, title, **kwargs)
            elif chart_type == "scatter":
                chart_result = await self._create_scatter_chart(df, x_column, y_column, group_by, title, **kwargs)
            elif chart_type == "pie":
                chart_result = await self._create_pie_chart(df, x_column, y_column, title, **kwargs)
            elif chart_type == "histogram":
                chart_result = await self._create_histogram(df, x_column, title, **kwargs)
            elif chart_type == "box":
                chart_result = await self._create_box_chart(df, x_column, y_column, group_by, title, **kwargs)
            elif chart_type == "heatmap":
                chart_result = await self._create_heatmap(df, title, **kwargs)
            else:
                return {"success": False, "error": f"Unsupported chart type: {chart_type}"}
            
            # Export chart
            export_path = await self._export_chart(
                chart_result["figure"], 
                chart_type, 
                export_format,
                title or f"{chart_type}_chart"
            )
            
            duration = time.time() - start_time
            
            result = {
                "success": True,
                "chart_type": chart_type,
                "data_source": data_source,
                "export_path": str(export_path),
                "export_format": export_format,
                "title": title or chart_result.get("title", ""),
                "rows_plotted": len(df),
                "duration_seconds": duration,
                "chart_info": chart_result.get("info", {}),
                "timestamp": datetime.now().isoformat()
            }
            
            # Log visualization
            analytics_logger.log_visualization(
                chart_type,
                data_source,
                str(export_path)
            )
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            analytics_logger.log_error("chart_creation", e, {
                "data_source": data_source,
                "chart_type": chart_type
            })
            
            return {
                "success": False,
                "error": f"Chart creation failed: {str(e)}",
                "duration_seconds": duration
            }
    
    async def create_dashboard(
        self,
        data_source: str,
        dashboard_type: str,
        metrics: Optional[List[str]] = None,
        time_range: Optional[str] = None,
        export_format: str = "html",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create comprehensive dashboard with multiple visualizations
        
        Args:
            data_source: Data source identifier
            dashboard_type: Type of dashboard
            metrics: Metrics to include
            time_range: Time range for analysis
            export_format: Export format
            
        Returns:
            Dashboard creation results
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
            
            # Create dashboard based on type
            if dashboard_type == "sales":
                dashboard_result = await self._create_sales_dashboard(df, metrics, time_range, **kwargs)
            elif dashboard_type == "financial":
                dashboard_result = await self._create_financial_dashboard(df, metrics, time_range, **kwargs)
            elif dashboard_type == "operational":
                dashboard_result = await self._create_operational_dashboard(df, metrics, time_range, **kwargs)
            elif dashboard_type == "custom":
                dashboard_result = await self._create_custom_dashboard(df, metrics, **kwargs)
            else:
                return {"success": False, "error": f"Unsupported dashboard type: {dashboard_type}"}
            
            # Export dashboard
            export_path = await self._export_dashboard(
                dashboard_result["figure"],
                dashboard_type,
                export_format
            )
            
            duration = time.time() - start_time
            
            result = {
                "success": True,
                "dashboard_type": dashboard_type,
                "data_source": data_source,
                "export_path": str(export_path),
                "export_format": export_format,
                "metrics_included": metrics or dashboard_result.get("metrics", []),
                "charts_created": dashboard_result.get("chart_count", 0),
                "rows_analyzed": len(df),
                "duration_seconds": duration,
                "timestamp": datetime.now().isoformat()
            }
            
            # Log dashboard creation
            analytics_logger.log_visualization(
                f"dashboard_{dashboard_type}",
                data_source,
                str(export_path)
            )
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            analytics_logger.log_error("dashboard_creation", e, {
                "data_source": data_source,
                "dashboard_type": dashboard_type
            })
            
            return {
                "success": False,
                "error": f"Dashboard creation failed: {str(e)}",
                "duration_seconds": duration
            }
    
    async def _create_bar_chart(self, df: pd.DataFrame, x_col: str, y_col: str, group_by: Optional[str], title: str, **kwargs) -> Dict[str, Any]:
        """Create bar chart"""
        
        if not x_col or not y_col:
            # Auto-select columns
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            categorical_cols = df.select_dtypes(include=['object']).columns
            
            x_col = x_col or (categorical_cols[0] if len(categorical_cols) > 0 else df.columns[0])
            y_col = y_col or (numeric_cols[0] if len(numeric_cols) > 0 else df.columns[1])
        
        if group_by:
            fig = px.bar(df, x=x_col, y=y_col, color=group_by, 
                        title=title or f"{y_col} by {x_col} (grouped by {group_by})",
                        color_discrete_sequence=self.color_palette)
        else:
            fig = px.bar(df, x=x_col, y=y_col,
                        title=title or f"{y_col} by {x_col}",
                        color_discrete_sequence=self.color_palette)
        
        fig.update_layout(
            template=self.default_theme,
            width=self.default_width,
            height=self.default_height
        )
        
        return {
            "figure": fig,
            "title": title or f"{y_col} by {x_col}",
            "info": {"x_column": x_col, "y_column": y_col, "group_by": group_by}
        }
    
    async def _create_line_chart(self, df: pd.DataFrame, x_col: str, y_col: str, group_by: Optional[str], title: str, **kwargs) -> Dict[str, Any]:
        """Create line chart"""
        
        if not x_col or not y_col:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            date_cols = df.select_dtypes(include=['datetime64']).columns
            
            x_col = x_col or (date_cols[0] if len(date_cols) > 0 else df.columns[0])
            y_col = y_col or (numeric_cols[0] if len(numeric_cols) > 0 else df.columns[1])
        
        # Try to convert x column to datetime if it looks like dates
        if df[x_col].dtype == 'object':
            try:
                df[x_col] = pd.to_datetime(df[x_col])
            except:
                pass
        
        if group_by:
            fig = px.line(df, x=x_col, y=y_col, color=group_by,
                         title=title or f"{y_col} over {x_col} (by {group_by})",
                         color_discrete_sequence=self.color_palette)
        else:
            fig = px.line(df, x=x_col, y=y_col,
                         title=title or f"{y_col} over {x_col}",
                         color_discrete_sequence=self.color_palette)
        
        fig.update_layout(
            template=self.default_theme,
            width=self.default_width,
            height=self.default_height
        )
        
        return {
            "figure": fig,
            "title": title or f"{y_col} over {x_col}",
            "info": {"x_column": x_col, "y_column": y_col, "group_by": group_by}
        }
    
    async def _create_scatter_chart(self, df: pd.DataFrame, x_col: str, y_col: str, group_by: Optional[str], title: str, **kwargs) -> Dict[str, Any]:
        """Create scatter plot"""
        
        if not x_col or not y_col:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) < 2:
                raise ValueError("Need at least 2 numeric columns for scatter plot")
            x_col = x_col or numeric_cols[0]
            y_col = y_col or numeric_cols[1]
        
        if group_by:
            fig = px.scatter(df, x=x_col, y=y_col, color=group_by,
                           title=title or f"{y_col} vs {x_col} (by {group_by})",
                           color_discrete_sequence=self.color_palette)
        else:
            fig = px.scatter(df, x=x_col, y=y_col,
                           title=title or f"{y_col} vs {x_col}",
                           color_discrete_sequence=self.color_palette)
        
        fig.update_layout(
            template=self.default_theme,
            width=self.default_width,
            height=self.default_height
        )
        
        return {
            "figure": fig,
            "title": title or f"{y_col} vs {x_col}",
            "info": {"x_column": x_col, "y_column": y_col, "group_by": group_by}
        }
    
    async def _create_pie_chart(self, df: pd.DataFrame, x_col: str, y_col: Optional[str], title: str, **kwargs) -> Dict[str, Any]:
        """Create pie chart"""
        
        if not x_col:
            categorical_cols = df.select_dtypes(include=['object']).columns
            x_col = categorical_cols[0] if len(categorical_cols) > 0 else df.columns[0]
        
        if y_col and y_col in df.columns:
            # Use y_col for values
            pie_data = df.groupby(x_col)[y_col].sum().reset_index()
            fig = px.pie(pie_data, values=y_col, names=x_col,
                        title=title or f"{y_col} distribution by {x_col}",
                        color_discrete_sequence=self.color_palette)
        else:
            # Use counts
            value_counts = df[x_col].value_counts().reset_index()
            value_counts.columns = [x_col, 'count']
            fig = px.pie(value_counts, values='count', names=x_col,
                        title=title or f"Distribution of {x_col}",
                        color_discrete_sequence=self.color_palette)
        
        fig.update_layout(
            template=self.default_theme,
            width=self.default_width,
            height=self.default_height
        )
        
        return {
            "figure": fig,
            "title": title or f"Distribution of {x_col}",
            "info": {"category_column": x_col, "value_column": y_col}
        }
    
    async def _create_histogram(self, df: pd.DataFrame, x_col: str, title: str, **kwargs) -> Dict[str, Any]:
        """Create histogram"""
        
        if not x_col:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            x_col = numeric_cols[0] if len(numeric_cols) > 0 else df.columns[0]
        
        fig = px.histogram(df, x=x_col, 
                          title=title or f"Distribution of {x_col}",
                          color_discrete_sequence=self.color_palette)
        
        fig.update_layout(
            template=self.default_theme,
            width=self.default_width,
            height=self.default_height
        )
        
        return {
            "figure": fig,
            "title": title or f"Distribution of {x_col}",
            "info": {"column": x_col}
        }
    
    async def _create_box_chart(self, df: pd.DataFrame, x_col: Optional[str], y_col: str, group_by: Optional[str], title: str, **kwargs) -> Dict[str, Any]:
        """Create box plot"""
        
        if not y_col:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            y_col = numeric_cols[0] if len(numeric_cols) > 0 else df.columns[0]
        
        if x_col:
            fig = px.box(df, x=x_col, y=y_col,
                        title=title or f"{y_col} distribution by {x_col}",
                        color_discrete_sequence=self.color_palette)
        else:
            fig = px.box(df, y=y_col,
                        title=title or f"{y_col} distribution",
                        color_discrete_sequence=self.color_palette)
        
        fig.update_layout(
            template=self.default_theme,
            width=self.default_width,
            height=self.default_height
        )
        
        return {
            "figure": fig,
            "title": title or f"{y_col} distribution",
            "info": {"x_column": x_col, "y_column": y_col}
        }
    
    async def _create_heatmap(self, df: pd.DataFrame, title: str, **kwargs) -> Dict[str, Any]:
        """Create correlation heatmap"""
        
        # Select only numeric columns
        numeric_df = df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            raise ValueError("Need at least 2 numeric columns for heatmap")
        
        correlation_matrix = numeric_df.corr()
        
        fig = px.imshow(correlation_matrix,
                       title=title or "Correlation Heatmap",
                       color_continuous_scale="RdBu_r",
                       aspect="auto")
        
        fig.update_layout(
            template=self.default_theme,
            width=self.default_width,
            height=self.default_height
        )
        
        return {
            "figure": fig,
            "title": title or "Correlation Heatmap",
            "info": {"columns": list(numeric_df.columns)}
        }
    
    async def _create_sales_dashboard(self, df: pd.DataFrame, metrics: Optional[List[str]], time_range: Optional[str], **kwargs) -> Dict[str, Any]:
        """Create sales dashboard"""
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Revenue Over Time", "Sales by Region", "Top Products", "Customer Distribution"),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"type": "pie"}]]
        )
        
        # Assume certain column names exist (would be more dynamic in real implementation)
        if "date" in df.columns and "amount" in df.columns:
            # Revenue over time
            daily_revenue = df.groupby("date")["amount"].sum().reset_index()
            fig.add_trace(
                go.Scatter(x=daily_revenue["date"], y=daily_revenue["amount"], 
                          mode='lines+markers', name="Revenue"),
                row=1, col=1
            )
        
        if "region" in df.columns and "amount" in df.columns:
            # Sales by region
            region_sales = df.groupby("region")["amount"].sum().reset_index()
            fig.add_trace(
                go.Bar(x=region_sales["region"], y=region_sales["amount"], name="Region Sales"),
                row=1, col=2
            )
        
        # Add more charts as needed...
        
        fig.update_layout(
            title="Sales Dashboard",
            template=self.default_theme,
            height=800,
            showlegend=False
        )
        
        return {
            "figure": fig,
            "chart_count": 4,
            "metrics": metrics or ["revenue", "regions", "products", "customers"]
        }
    
    async def _create_financial_dashboard(self, df: pd.DataFrame, metrics: Optional[List[str]], time_range: Optional[str], **kwargs) -> Dict[str, Any]:
        """Create financial dashboard"""
        
        # Similar structure to sales dashboard but with financial metrics
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Profit/Loss", "Cash Flow", "Expenses", "Budget vs Actual")
        )
        
        # Add financial charts...
        
        fig.update_layout(
            title="Financial Dashboard",
            template=self.default_theme,
            height=800
        )
        
        return {
            "figure": fig,
            "chart_count": 4,
            "metrics": metrics or ["profit", "cashflow", "expenses", "budget"]
        }
    
    async def _create_operational_dashboard(self, df: pd.DataFrame, metrics: Optional[List[str]], time_range: Optional[str], **kwargs) -> Dict[str, Any]:
        """Create operational dashboard"""
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Performance Metrics", "Resource Utilization", "Quality Indicators", "Efficiency Trends")
        )
        
        # Add operational charts...
        
        fig.update_layout(
            title="Operational Dashboard",
            template=self.default_theme,
            height=800
        )
        
        return {
            "figure": fig,
            "chart_count": 4,
            "metrics": metrics or ["performance", "utilization", "quality", "efficiency"]
        }
    
    async def _create_custom_dashboard(self, df: pd.DataFrame, metrics: Optional[List[str]], **kwargs) -> Dict[str, Any]:
        """Create custom dashboard"""
        
        # Create a simple overview dashboard
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) >= 2:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=("Data Overview", "Correlations", "Distributions", "Summary Stats")
            )
            
            # Add basic charts
            fig.add_trace(
                go.Scatter(x=df.index, y=df[numeric_cols[0]], mode='lines', name=numeric_cols[0]),
                row=1, col=1
            )
            
            fig.update_layout(
                title="Custom Dashboard",
                template=self.default_theme,
                height=800
            )
        else:
            # Simple single chart
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df.index[:10], y=df.iloc[:10, 0], name="Data"))
            fig.update_layout(title="Custom Dashboard", template=self.default_theme)
        
        return {
            "figure": fig,
            "chart_count": 1,
            "metrics": metrics or ["overview"]
        }
    
    async def _export_chart(self, figure, chart_type: str, export_format: str, title: str) -> Path:
        """Export chart to file"""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{chart_type}_{title.replace(' ', '_')}_{timestamp}.{export_format}"
        export_path = self.output_dir / filename
        
        if export_format == "html":
            figure.write_html(str(export_path))
        elif export_format == "png":
            figure.write_image(str(export_path), format="png")
        elif export_format == "svg":
            figure.write_image(str(export_path), format="svg")
        else:
            raise ValueError(f"Unsupported export format: {export_format}")
        
        return export_path
    
    async def _export_dashboard(self, figure, dashboard_type: str, export_format: str) -> Path:
        """Export dashboard to file"""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dashboard_{dashboard_type}_{timestamp}.{export_format}"
        export_path = self.output_dir / filename
        
        if export_format == "html":
            figure.write_html(str(export_path))
        elif export_format == "png":
            figure.write_image(str(export_path), format="png", width=1200, height=800)
        else:
            raise ValueError(f"Unsupported export format for dashboard: {export_format}")
        
        return export_path
    
    async def _get_data_from_source(self, data_source: str) -> Optional[List[Dict[str, Any]]]:
        """Get data from data source (placeholder)"""
        
        # Placeholder - would get data from DataTools in real implementation
        if "sales" in data_source.lower():
            return [
                {"date": "2024-01-01", "amount": 1000, "region": "North", "customer": "A", "product": "Widget"},
                {"date": "2024-01-02", "amount": 1500, "region": "South", "customer": "B", "product": "Gadget"},
                {"date": "2024-01-03", "amount": 800, "region": "North", "customer": "C", "product": "Widget"},
                {"date": "2024-01-04", "amount": 2000, "region": "East", "customer": "D", "product": "Tool"},
                {"date": "2024-01-05", "amount": 1200, "region": "West", "customer": "E", "product": "Gadget"}
            ]
        
        return None 