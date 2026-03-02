"""
Data Tools for AI Data Analyst

Provides unified interface to access data from various sources including
databases and files, with caching and data source management.
"""

from typing import Any, Dict, List, Optional, Union
import time
import asyncio

from data_sources.database_connector import DatabaseConnector
from data_sources.file_reader import FileReader
from utils.config_manager import ConfigManager
from utils.logger import setup_logger, AnalyticsLogger

logger = setup_logger(__name__)
analytics_logger = AnalyticsLogger("data_tools")


class DataTools:
    """Unified data access tools for databases and files"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.db_connector = DatabaseConnector(config)
        self.file_reader = FileReader(config)
        self._data_sources = {}
        self._initialized = False
    
    async def initialize(self):
        """Initialize data connections"""
        if self._initialized:
            return
        
        logger.info("Initializing data tools...")
        
        # Initialize database connections
        await self.db_connector.initialize()
        
        # Register available data sources
        await self._register_data_sources()
        
        self._initialized = True
        logger.info("Data tools initialized successfully")
    
    async def _register_data_sources(self):
        """Register all available data sources"""
        
        # Register database sources
        enabled_dbs = self.config.get_enabled_databases()
        for db_name in enabled_dbs:
            try:
                tables = await self.db_connector.list_tables(db_name)
                for table in tables:
                    source_id = f"db_{db_name}_{table}"
                    self._data_sources[source_id] = {
                        "type": "database",
                        "database": db_name,
                        "table": table,
                        "source_id": source_id,
                        "description": f"Table {table} in {db_name} database"
                    }
            except Exception as e:
                logger.error(f"Failed to register tables from {db_name}: {str(e)}")
        
        logger.info(f"Registered {len(self._data_sources)} data sources")
    
    async def query_database(
        self,
        query: str,
        database: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute SQL query on database
        
        Args:
            query: SQL query to execute
            database: Database name (optional)
            limit: Row limit (optional)
            
        Returns:
            Query results with metadata
        """
        
        if not self._initialized:
            await self.initialize()
        
        logger.info(f"Executing database query: {query[:100]}...")
        
        try:
            result = await self.db_connector.execute_query(
                query=query,
                database=database,
                limit=limit
            )
            
            if result["success"]:
                # Register as temporary data source if it's a SELECT query
                if query.strip().upper().startswith("SELECT"):
                    temp_source_id = f"query_result_{int(time.time())}"
                    self._data_sources[temp_source_id] = {
                        "type": "query_result",
                        "query": query,
                        "database": result["database"],
                        "source_id": temp_source_id,
                        "description": f"Query result from {result['database']}",
                        "data": result["data"],
                        "columns": result["columns"]
                    }
                    result["source_id"] = temp_source_id
            
            return result
            
        except Exception as e:
            analytics_logger.log_error("database_query", e, {"query": query[:100]})
            return {
                "success": False,
                "error": f"Database query failed: {str(e)}"
            }
    
    async def read_file(
        self,
        file_path: str,
        sheet_name: Optional[str] = None,
        encoding: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Read data from file
        
        Args:
            file_path: Path to the file
            sheet_name: Excel sheet name (optional)
            encoding: File encoding (optional)
            
        Returns:
            File data with metadata
        """
        
        logger.info(f"Reading file: {file_path}")
        
        try:
            result = await self.file_reader.read_file(
                file_path=file_path,
                sheet_name=sheet_name,
                encoding=encoding
            )
            
            if result["success"]:
                # Register as data source
                source_id = f"file_{file_path.replace('/', '_').replace('\\', '_')}"
                self._data_sources[source_id] = {
                    "type": "file",
                    "file_path": file_path,
                    "source_id": source_id,
                    "description": f"File data from {file_path}",
                    "data": result["data"],
                    "columns": result["columns"],
                    "metadata": result["metadata"]
                }
                result["source_id"] = source_id
            
            return result
            
        except Exception as e:
            analytics_logger.log_error("file_reading", e, {"file_path": file_path})
            return {
                "success": False,
                "error": f"File reading failed: {str(e)}"
            }
    
    async def list_data_sources(self) -> Dict[str, Any]:
        """
        List all available data sources
        
        Returns:
            Dictionary of available data sources
        """
        
        if not self._initialized:
            await self.initialize()
        
        # Group data sources by type
        sources_by_type = {
            "databases": {},
            "files": {},
            "query_results": {}
        }
        
        for source_id, source_info in self._data_sources.items():
            if source_info["type"] == "database":
                db_name = source_info["database"]
                if db_name not in sources_by_type["databases"]:
                    sources_by_type["databases"][db_name] = []
                sources_by_type["databases"][db_name].append({
                    "source_id": source_id,
                    "table": source_info["table"],
                    "description": source_info["description"]
                })
            
            elif source_info["type"] == "file":
                sources_by_type["files"][source_id] = {
                    "file_path": source_info["file_path"],
                    "description": source_info["description"],
                    "columns": source_info["columns"],
                    "row_count": len(source_info["data"])
                }
            
            elif source_info["type"] == "query_result":
                sources_by_type["query_results"][source_id] = {
                    "query": source_info["query"][:100] + "...",
                    "database": source_info["database"],
                    "description": source_info["description"],
                    "columns": source_info["columns"],
                    "row_count": len(source_info["data"])
                }
        
        return {
            "success": True,
            "data_sources": sources_by_type,
            "total_sources": len(self._data_sources)
        }
    
    async def get_data_source_info(self, source_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific data source
        
        Args:
            source_id: Data source identifier
            
        Returns:
            Detailed source information
        """
        
        if source_id not in self._data_sources:
            return {
                "success": False,
                "error": f"Data source not found: {source_id}"
            }
        
        source_info = self._data_sources[source_id].copy()
        
        # Add sample data
        if "data" in source_info and source_info["data"]:
            source_info["sample_data"] = source_info["data"][:5]  # First 5 rows
            source_info["row_count"] = len(source_info["data"])
        
        # Add schema information for database sources
        if source_info["type"] == "database":
            try:
                schema = await self.db_connector.get_table_schema(
                    source_info["table"],
                    source_info["database"]
                )
                source_info["schema"] = schema
            except Exception as e:
                logger.error(f"Failed to get schema for {source_id}: {str(e)}")
        
        return {
            "success": True,
            "source_info": source_info
        }
    
    async def get_data_sample(self, source_id: str, limit: int = 10) -> Dict[str, Any]:
        """
        Get sample data from a data source
        
        Args:
            source_id: Data source identifier
            limit: Number of rows to return
            
        Returns:
            Sample data
        """
        
        if source_id not in self._data_sources:
            return {
                "success": False,
                "error": f"Data source not found: {source_id}"
            }
        
        source_info = self._data_sources[source_id]
        
        try:
            if source_info["type"] == "database":
                # Get sample from database
                result = await self.db_connector.get_sample_data(
                    source_info["table"],
                    limit,
                    source_info["database"]
                )
                return result
            
            elif source_info["type"] in ["file", "query_result"]:
                # Get sample from cached data
                data = source_info["data"][:limit]
                return {
                    "success": True,
                    "data": data,
                    "columns": source_info["columns"],
                    "row_count": len(data),
                    "source_id": source_id
                }
            
            else:
                return {
                    "success": False,
                    "error": f"Unsupported source type: {source_info['type']}"
                }
                
        except Exception as e:
            analytics_logger.log_error("get_data_sample", e, {"source_id": source_id})
            return {
                "success": False,
                "error": f"Failed to get sample data: {str(e)}"
            }
    
    def get_data_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get data source by ID"""
        return self._data_sources.get(source_id)
    
    def get_data_source_data(self, source_id: str) -> Optional[List[Dict[str, Any]]]:
        """Get the actual data from a data source"""
        source = self.get_data_source(source_id)
        if source and "data" in source:
            return source["data"]
        return None
    
    def register_temp_data_source(self, data: List[Dict[str, Any]], columns: List[str], description: str = "") -> str:
        """Register temporary data source"""
        source_id = f"temp_{int(time.time())}"
        self._data_sources[source_id] = {
            "type": "temporary",
            "source_id": source_id,
            "description": description or "Temporary data source",
            "data": data,
            "columns": columns
        }
        return source_id
    
    def cleanup_temp_sources(self):
        """Clean up temporary data sources"""
        temp_sources = [sid for sid, info in self._data_sources.items() 
                       if info["type"] in ["query_result", "temporary"]]
        
        for source_id in temp_sources:
            del self._data_sources[source_id]
        
        logger.info(f"Cleaned up {len(temp_sources)} temporary data sources")
    
    async def close(self):
        """Close all data connections"""
        await self.db_connector.close_connections()
        self.file_reader.clear_cache()
        self._data_sources.clear()
        self._initialized = False
        logger.info("Data tools closed") 