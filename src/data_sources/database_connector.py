"""
Database Connector for AI Data Analyst

Provides unified interface for connecting to various SQL databases
including SQLite, PostgreSQL, MySQL, and Snowflake.
"""

import sqlite3
import asyncio
from typing import Any, Dict, List, Optional, Union, Tuple
from contextlib import asynccontextmanager
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, inspect
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import time

from utils.config_manager import ConfigManager
from utils.logger import setup_logger, AnalyticsLogger

logger = setup_logger(__name__)
analytics_logger = AnalyticsLogger("database")


class DatabaseConnector:
    """Unified database connector supporting multiple database types"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.connections = {}
        self.engines = {}
        self._initialized = False
    
    async def initialize(self):
        """Initialize database connections"""
        if self._initialized:
            return
        
        enabled_dbs = self.config.get_enabled_databases()
        logger.info(f"Initializing {len(enabled_dbs)} database connections")
        
        for db_name, db_config in enabled_dbs.items():
            try:
                engine = await self._create_engine(db_name, db_config)
                self.engines[db_name] = engine
                logger.info(f"Connected to {db_name} database")
            except Exception as e:
                logger.error(f"Failed to connect to {db_name}: {str(e)}")
        
        self._initialized = True
    
    async def _create_engine(self, db_name: str, db_config: Dict[str, Any]):
        """Create database engine based on database type"""
        
        if db_name == "sqlite":
            db_path = db_config["path"]
            connection_string = f"sqlite:///{db_path}"
            return create_engine(connection_string, echo=False)
        
        elif db_name == "postgresql":
            connection_string = (
                f"postgresql://{db_config['username']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
            return create_engine(connection_string, echo=False)
        
        elif db_name == "mysql":
            connection_string = (
                f"mysql+pymysql://{db_config['username']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
            return create_engine(connection_string, echo=False)
        
        elif db_name == "snowflake":
            connection_string = (
                f"snowflake://{db_config['username']}:{db_config['password']}"
                f"@{db_config['account']}/{db_config['database']}/{db_config['schema']}"
                f"?warehouse={db_config['warehouse']}"
            )
            return create_engine(connection_string, echo=False)
        
        else:
            raise ValueError(f"Unsupported database type: {db_name}")
    
    async def execute_query(
        self, 
        query: str, 
        database: Optional[str] = None, 
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute SQL query and return results
        
        Args:
            query: SQL query to execute
            database: Database name (uses primary if not specified)
            params: Query parameters
            limit: Maximum number of rows to return
            
        Returns:
            Dictionary with query results and metadata
        """
        
        if not self._initialized:
            await self.initialize()
        
        # Get database engine
        db_name = database or self._get_primary_database()
        engine = self.engines.get(db_name)
        
        if not engine:
            raise ValueError(f"Database {db_name} not available")
        
        # Add limit to query if specified
        if limit and not self._has_limit_clause(query):
            query = f"{query.rstrip(';')} LIMIT {limit}"
        
        start_time = time.time()
        
        try:
            # Execute query
            with engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                
                # Fetch results
                if result.returns_rows:
                    columns = list(result.keys())
                    rows = result.fetchall()
                    
                    # Convert to list of dictionaries
                    data = [dict(zip(columns, row)) for row in rows]
                    
                    duration = time.time() - start_time
                    
                    # Log query execution
                    analytics_logger.log_query(query, duration, len(data))
                    
                    return {
                        "success": True,
                        "data": data,
                        "columns": columns,
                        "row_count": len(data),
                        "duration_seconds": duration,
                        "database": db_name
                    }
                else:
                    # Non-SELECT query
                    duration = time.time() - start_time
                    
                    return {
                        "success": True,
                        "message": "Query executed successfully",
                        "rows_affected": result.rowcount,
                        "duration_seconds": duration,
                        "database": db_name
                    }
                    
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Query execution failed: {str(e)}"
            
            analytics_logger.log_error("query_execution", e, {
                "query": query[:100],
                "database": db_name,
                "duration": duration
            })
            
            return {
                "success": False,
                "error": error_msg,
                "duration_seconds": duration,
                "database": db_name
            }
    
    async def get_table_schema(self, table_name: str, database: Optional[str] = None) -> Dict[str, Any]:
        """Get schema information for a table"""
        
        if not self._initialized:
            await self.initialize()
        
        db_name = database or self._get_primary_database()
        engine = self.engines.get(db_name)
        
        if not engine:
            raise ValueError(f"Database {db_name} not available")
        
        try:
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            
            schema_info = {
                "table_name": table_name,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col["nullable"],
                        "default": col.get("default")
                    }
                    for col in columns
                ],
                "database": db_name
            }
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            return {"error": str(e)}
    
    async def list_tables(self, database: Optional[str] = None) -> List[str]:
        """List all tables in the database"""
        
        if not self._initialized:
            await self.initialize()
        
        db_name = database or self._get_primary_database()
        engine = self.engines.get(db_name)
        
        if not engine:
            raise ValueError(f"Database {db_name} not available")
        
        try:
            inspector = inspect(engine)
            return inspector.get_table_names()
            
        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
            return []
    
    async def get_sample_data(self, table_name: str, limit: int = 10, database: Optional[str] = None) -> Dict[str, Any]:
        """Get sample data from a table"""
        
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return await self.execute_query(query, database)
    
    def _get_primary_database(self) -> str:
        """Get the primary database name"""
        enabled_dbs = list(self.config.get_enabled_databases().keys())
        
        if not enabled_dbs:
            raise ValueError("No databases are enabled")
        
        # Prefer SQLite for simplicity, then others
        if "sqlite" in enabled_dbs:
            return "sqlite"
        
        return enabled_dbs[0]
    
    def _has_limit_clause(self, query: str) -> bool:
        """Check if query already has a LIMIT clause"""
        return "LIMIT" in query.upper()
    
    async def close_connections(self):
        """Close all database connections"""
        for db_name, engine in self.engines.items():
            try:
                engine.dispose()
                logger.info(f"Closed connection to {db_name}")
            except Exception as e:
                logger.error(f"Error closing connection to {db_name}: {str(e)}")
        
        self.engines.clear()
        self._initialized = False


class QueryBuilder:
    """Helper class for building SQL queries"""
    
    @staticmethod
    def build_select_query(
        table: str,
        columns: Optional[List[str]] = None,
        where_conditions: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        group_by: Optional[List[str]] = None
    ) -> str:
        """Build a SELECT query"""
        
        # SELECT clause
        if columns:
            select_clause = f"SELECT {', '.join(columns)}"
        else:
            select_clause = "SELECT *"
        
        # FROM clause
        query = f"{select_clause} FROM {table}"
        
        # WHERE clause
        if where_conditions:
            conditions = []
            for column, value in where_conditions.items():
                if isinstance(value, str):
                    conditions.append(f"{column} = '{value}'")
                elif isinstance(value, (list, tuple)):
                    value_str = "', '".join(str(v) for v in value)
                    conditions.append(f"{column} IN ('{value_str}')")
                else:
                    conditions.append(f"{column} = {value}")
            
            query += f" WHERE {' AND '.join(conditions)}"
        
        # GROUP BY clause
        if group_by:
            query += f" GROUP BY {', '.join(group_by)}"
        
        # ORDER BY clause
        if order_by:
            query += f" ORDER BY {order_by}"
        
        # LIMIT clause
        if limit:
            query += f" LIMIT {limit}"
        
        return query
    
    @staticmethod
    def build_aggregation_query(
        table: str,
        aggregations: Dict[str, str],  # {"column": "function"}
        group_by: Optional[List[str]] = None,
        where_conditions: Optional[Dict[str, Any]] = None
    ) -> str:
        """Build an aggregation query"""
        
        # Build SELECT with aggregations
        agg_clauses = []
        for column, func in aggregations.items():
            agg_clauses.append(f"{func.upper()}({column}) as {func}_{column}")
        
        if group_by:
            select_columns = group_by + agg_clauses
        else:
            select_columns = agg_clauses
        
        return QueryBuilder.build_select_query(
            table=table,
            columns=select_columns,
            where_conditions=where_conditions,
            group_by=group_by
        ) 