"""
File Reader for AI Data Analyst

Handles reading and parsing CSV, Excel, JSON files with data validation
and type inference capabilities.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import numpy as np
from datetime import datetime
import time

from utils.config_manager import ConfigManager
from utils.logger import setup_logger, AnalyticsLogger

logger = setup_logger(__name__)
analytics_logger = AnalyticsLogger("file_reader")


class FileReader:
    """File reader with support for multiple formats and data validation"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.supported_formats = config.get("files.supported_formats", ["csv", "xlsx", "xls", "json"])
        self.max_file_size_mb = config.get("files.max_file_size_mb", 100)
        self.encoding = config.get("files.encoding", "utf-8")
        self.csv_delimiter = config.get("files.csv_delimiter", ",")
        self.excel_sheet = config.get("files.excel_sheet", 0)
        
        # Cache for loaded files
        self._file_cache = {}
    
    async def read_file(
        self,
        file_path: str,
        sheet_name: Optional[str] = None,
        encoding: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Read file and return structured data
        
        Args:
            file_path: Path to the file
            sheet_name: Excel sheet name (optional)
            encoding: File encoding (optional)
            **kwargs: Additional parameters for file readers
            
        Returns:
            Dictionary with file data and metadata
        """
        
        start_time = time.time()
        file_path = Path(file_path)
        
        # Validate file
        validation_result = self._validate_file(file_path)
        if not validation_result["valid"]:
            return {
                "success": False,
                "error": validation_result["error"],
                "file_path": str(file_path)
            }
        
        # Check cache
        cache_key = self._get_cache_key(file_path, sheet_name, encoding)
        if cache_key in self._file_cache:
            logger.info(f"Returning cached data for {file_path}")
            return self._file_cache[cache_key]
        
        try:
            # Determine file type and read
            file_extension = file_path.suffix.lower()
            
            if file_extension == ".csv":
                data = await self._read_csv(file_path, encoding or self.encoding, **kwargs)
            elif file_extension in [".xlsx", ".xls"]:
                data = await self._read_excel(file_path, sheet_name, **kwargs)
            elif file_extension == ".json":
                data = await self._read_json(file_path, encoding or self.encoding, **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
            
            duration = time.time() - start_time
            
            # Prepare result
            result = {
                "success": True,
                "data": data["records"],
                "columns": data["columns"],
                "data_types": data["data_types"],
                "row_count": len(data["records"]),
                "file_path": str(file_path),
                "file_size_mb": file_path.stat().st_size / (1024 * 1024),
                "duration_seconds": duration,
                "metadata": {
                    "file_extension": file_extension,
                    "encoding": encoding or self.encoding,
                    "sheet_name": sheet_name,
                    "inferred_types": data.get("inferred_types", {}),
                    "null_counts": data.get("null_counts", {}),
                    "summary_stats": data.get("summary_stats", {})
                }
            }
            
            # Cache result
            self._file_cache[cache_key] = result
            
            # Log file reading
            analytics_logger.log_analysis(
                "file_reading",
                str(file_path),
                duration,
                f"Read {len(data['records'])} rows, {len(data['columns'])} columns"
            )
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed to read file: {str(e)}"
            
            analytics_logger.log_error("file_reading", e, {
                "file_path": str(file_path),
                "duration": duration
            })
            
            return {
                "success": False,
                "error": error_msg,
                "file_path": str(file_path),
                "duration_seconds": duration
            }
    
    async def _read_csv(self, file_path: Path, encoding: str, **kwargs) -> Dict[str, Any]:
        """Read CSV file"""
        
        # Default CSV parameters
        csv_params = {
            "delimiter": kwargs.get("delimiter", self.csv_delimiter),
            "encoding": encoding,
            "low_memory": False,
            "na_values": ["", "NA", "N/A", "NULL", "null", "None"]
        }
        
        # Update with user parameters
        csv_params.update({k: v for k, v in kwargs.items() if k in [
            "delimiter", "quotechar", "skiprows", "nrows", "usecols"
        ]})
        
        df = pd.read_csv(file_path, **csv_params)
        return self._process_dataframe(df)
    
    async def _read_excel(self, file_path: Path, sheet_name: Optional[str], **kwargs) -> Dict[str, Any]:
        """Read Excel file"""
        
        excel_params = {
            "sheet_name": sheet_name or self.excel_sheet,
            "na_values": ["", "NA", "N/A", "NULL", "null", "None"]
        }
        
        # Update with user parameters
        excel_params.update({k: v for k, v in kwargs.items() if k in [
            "skiprows", "nrows", "usecols", "header"
        ]})
        
        df = pd.read_excel(file_path, **excel_params)
        return self._process_dataframe(df)
    
    async def _read_json(self, file_path: Path, encoding: str, **kwargs) -> Dict[str, Any]:
        """Read JSON file"""
        
        with open(file_path, 'r', encoding=encoding) as file:
            json_data = json.load(file)
        
        # Handle different JSON structures
        if isinstance(json_data, list):
            # Array of objects
            df = pd.DataFrame(json_data)
        elif isinstance(json_data, dict):
            if "data" in json_data:
                # Structured format with data key
                df = pd.DataFrame(json_data["data"])
            else:
                # Single object - convert to single-row DataFrame
                df = pd.DataFrame([json_data])
        else:
            raise ValueError("Unsupported JSON structure")
        
        return self._process_dataframe(df)
    
    def _process_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Process DataFrame and extract metadata"""
        
        # Clean column names
        df.columns = df.columns.astype(str)
        df.columns = [col.strip() for col in df.columns]
        
        # Infer data types
        inferred_types = {}
        for col in df.columns:
            inferred_types[col] = self._infer_column_type(df[col])
        
        # Apply type conversions
        for col, dtype in inferred_types.items():
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif dtype == "numeric":
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif dtype == "boolean":
                df[col] = df[col].astype(bool)
        
        # Calculate statistics
        null_counts = df.isnull().sum().to_dict()
        summary_stats = {}
        
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64']:
                summary_stats[col] = {
                    "mean": float(df[col].mean()) if not pd.isna(df[col].mean()) else None,
                    "median": float(df[col].median()) if not pd.isna(df[col].median()) else None,
                    "std": float(df[col].std()) if not pd.isna(df[col].std()) else None,
                    "min": float(df[col].min()) if not pd.isna(df[col].min()) else None,
                    "max": float(df[col].max()) if not pd.isna(df[col].max()) else None
                }
            elif df[col].dtype == 'object':
                summary_stats[col] = {
                    "unique_count": int(df[col].nunique()),
                    "most_common": str(df[col].mode().iloc[0]) if not df[col].mode().empty else None
                }
        
        # Convert DataFrame to records
        records = df.fillna(None).to_dict('records')
        
        return {
            "records": records,
            "columns": list(df.columns),
            "data_types": {col: str(df[col].dtype) for col in df.columns},
            "inferred_types": inferred_types,
            "null_counts": null_counts,
            "summary_stats": summary_stats
        }
    
    def _infer_column_type(self, series: pd.Series) -> str:
        """Infer the semantic type of a column"""
        
        # Remove null values for analysis
        clean_series = series.dropna()
        
        if len(clean_series) == 0:
            return "unknown"
        
        # Check if numeric
        if pd.api.types.is_numeric_dtype(clean_series):
            return "numeric"
        
        # Check if datetime
        try:
            pd.to_datetime(clean_series.head(10))
            return "datetime"
        except:
            pass
        
        # Check if boolean
        if clean_series.dtype == bool or all(val in [True, False, 'true', 'false', 'True', 'False', 1, 0] for val in clean_series.head(20)):
            return "boolean"
        
        # Check if categorical (limited unique values)
        unique_ratio = clean_series.nunique() / len(clean_series)
        if unique_ratio < 0.1 and clean_series.nunique() < 20:
            return "categorical"
        
        return "text"
    
    def _validate_file(self, file_path: Path) -> Dict[str, Any]:
        """Validate file before reading"""
        
        if not file_path.exists():
            return {"valid": False, "error": f"File not found: {file_path}"}
        
        if not file_path.is_file():
            return {"valid": False, "error": f"Path is not a file: {file_path}"}
        
        # Check file extension
        file_extension = file_path.suffix.lower().replace(".", "")
        if file_extension not in self.supported_formats:
            return {
                "valid": False, 
                "error": f"Unsupported file format: {file_extension}. Supported: {self.supported_formats}"
            }
        
        # Check file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        if file_size_mb > self.max_file_size_mb:
            return {
                "valid": False,
                "error": f"File too large: {file_size_mb:.1f}MB. Maximum allowed: {self.max_file_size_mb}MB"
            }
        
        return {"valid": True}
    
    def _get_cache_key(self, file_path: Path, sheet_name: Optional[str], encoding: Optional[str]) -> str:
        """Generate cache key for file"""
        
        # Include file modification time to invalidate cache when file changes
        mtime = file_path.stat().st_mtime
        return f"{file_path}_{sheet_name}_{encoding}_{mtime}"
    
    def clear_cache(self):
        """Clear file cache"""
        self._file_cache.clear()
        logger.info("File cache cleared")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get cache information"""
        return {
            "cached_files": len(self._file_cache),
            "cache_keys": list(self._file_cache.keys())
        }
    
    async def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get basic file information without reading the content"""
        
        file_path = Path(file_path)
        
        if not file_path.exists():
            return {"error": f"File not found: {file_path}"}
        
        stat = file_path.stat()
        
        return {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "file_extension": file_path.suffix,
            "file_size_bytes": stat.st_size,
            "file_size_mb": stat.st_size / (1024 * 1024),
            "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "is_supported": file_path.suffix.lower().replace(".", "") in self.supported_formats
        } 