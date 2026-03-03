# -*- coding: utf-8 -*-
"""
Main entry point for AI Data Analyst MCP Server
"""

import asyncio
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# Now import with absolute paths
from mcp_server.server import MCPDataAnalyst
from utils.logger import setup_logger

logger = setup_logger(__name__)

async def main():
    """Main entry point for the AI Data Analyst MCP Server"""
    
    try:
        logger.info("Starting AI Data Analyst MCP Server...")
        
        # Create and run the MCP server
        server = MCPDataAnalyst("config/config.yaml")
        await server.run()
        
    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        raise
    finally:
        logger.info("AI Data Analyst MCP Server stopped")

if __name__ == "__main__":
    asyncio.run(main())
