# AI-Powered Data Analyst

An MCP (Model Context Protocol) agent that understands natural language queries and performs data analysis on various data sources including SQL databases, CSV/Excel files, and APIs.

## Features

- **Natural Language Processing**: Convert plain English queries into data operations
- **Multi-Source Data Access**: Connect to SQL databases, Snowflake, CSV/Excel files, and REST APIs
- **Advanced Analytics**: Filtering, aggregation, statistical analysis, and anomaly detection
- **Visualization**: Generate charts, graphs, and interactive dashboards
- **MCP Integration**: Seamless integration with LLM tools via JSON-RPC protocol

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   LLM Client    │◄──►│   MCP Server     │◄──►│  Data Sources   │
│                 │    │                  │    │                 │
│ - Query Input   │    │ - Tool Routing   │    │ - SQL Database  │
│ - Response      │    │ - Data Processing│    │ - CSV/Excel     │
│   Formatting    │    │ - Analytics      │    │ - REST APIs     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │ Analytics Engine │
                       │                  │
                       │ - Query Executor │
                       │ - Chart Generator│
                       │ - Statistical    │
                       │   Analysis       │
                       └──────────────────┘
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd ai-data-analyst
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Starting the MCP Server

```bash
python src/main.py
```

### Example Queries

1. **Sales Analysis**
   - "What was our total revenue in Q2 across all regions?"
   - "Show me the top 5 products by sales volume"

2. **Customer Segmentation**
   - "List top 5 customers by lifetime value"
   - "Which customers haven't purchased in the last 6 months?"

3. **Trend Visualization**
   - "Show a line chart of monthly active users over the past year"
   - "Create a bar chart of sales by region"

4. **Anomaly Detection**
   - "Were there any unusual spikes in expenses last month?"
   - "Find outliers in our daily transaction volumes"

## Project Structure

```
ai-data-analyst/
├── src/
│   ├── mcp_server/          # MCP JSON-RPC server implementation
│   ├── data_sources/        # Database and file connectors
│   ├── analytics/           # Analysis and visualization engine
│   ├── tools/              # MCP tool definitions
│   └── utils/              # Helper utilities
├── data/                   # Sample datasets
├── tests/                  # Unit and integration tests
├── config/                 # Configuration files
└── docs/                   # Documentation
```

## Configuration

Edit `config/config.yaml` to configure data sources:

```yaml
databases:
  sqlite:
    path: "data/sample.db"
  postgresql:
    host: "localhost"
    port: 5432
    database: "analytics"
    
visualization:
  default_theme: "plotly_white"
  export_format: "png"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details 