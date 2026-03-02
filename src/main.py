# -*- coding: utf-8 -*-
"""
Main entry point for AI Data Analyst FastMCP Server
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# Import and run the FastMCP server
from mcp_server.server import mcp, logger

if __name__ == "__main__":
    logger.info("Starting AI Data Analyst FastMCP Server...")
    # Use stdio transport for MCP clients
    mcp.run(transport="stdio")

