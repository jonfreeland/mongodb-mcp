# MongoDB MCP Server

A Model Context Protocol server that provides read-only access to MongoDB databases through standardized MCP tools and resources.

<a href="https://glama.ai/mcp/servers/cmywezu1sn">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/cmywezu1sn/badge" alt="MongoDB Server MCP server" />
</a>

## Overview

This MongoDB MCP server enables AI assistants to directly query and analyze MongoDB databases without write access, maintaining data safety while providing powerful data exploration capabilities.

## Features

### MongoDB Operations
- **Database Exploration**: List databases and collections
- **Schema Discovery**: Infer collection schemas from sample documents
- **Querying**: Execute MongoDB queries with filtering, projection, sorting, and limiting
- **Aggregation**: Run read-only aggregation pipelines with safety validation
- **Text Search**: Perform full-text search on collections with text indexes
- **Geospatial Queries**: Find locations near points, within polygons, or intersecting geometries
- **Document Operations**: Count documents, sample random documents, find documents by IDs
- **Data Analysis**: Get collection statistics, index information, and query execution plans
- **Performance Insights**: Examine query execution plans to optimize performance
- **Data Exploration**: Get distinct values, field distributions, and data samples
- **Format Conversion**: Export query results as JSON or CSV formats

### Enhanced Capabilities
- **Schema Inference**: Automatically detect data types and structure from documents
- **Visualization Hints**: Intelligent suggestions for data visualization based on result content
- **Safety Validation**: Prevents write operations in aggregation pipelines
- **Example-Rich Documentation**: Each tool includes detailed examples in its description

## Requirements

### Environment Variables
- `MONGODB_URI` (required): MongoDB connection string with authentication if needed
- `MONGODB_DEFAULT_DATABASE` (optional): Default database name when not specified in queries

### Prerequisites
- Network access to MongoDB server
- Authentication credentials if required by MongoDB instance
- Appropriate read permissions on target databases

## Installation

### Building from Source

Install dependencies:
```bash
npm install
```

Build the server:
```bash
npm run build
```

For development with auto-rebuild:
```bash
npm run watch
```

### Integration with Claude Desktop

To use with Claude Desktop, add the server configuration:

On MacOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "mongodb": {
      "command": "/path/to/mongodb-server/build/index.js",
      "env": {
        "MONGODB_URI": "mongodb://username:password@hostname:port/database",
        "MONGODB_DEFAULT_DATABASE": "your_default_db"
      }
    }
  }
}
```

### Integration with Claude Web

For Claude Web via the MCP Chrome extension, add configuration to Cline MCP settings:

```json
{
  "mcpServers": {
    "mongodb": {
      "command": "node",
      "args": ["/path/to/mongodb-server/build/index.js"],
      "env": {
        "MONGODB_URI": "mongodb://username:password@hostname:port/database",
        "MONGODB_DEFAULT_DATABASE": "your_default_db"
      }
    }
  }
}
```

### Integration with Claude Code

To use with Claude Code, use the following commands:

```bash
cd /path/to/my/project
claude mcp add mongo-server /path/to/mongodb-mcp/build/index.js -e "MONGODB_URI=mongodb://user@password:27017/dbname?authSource=authDbName" -e MONGO_DEFAULT_DATABASE=dbname 
```

Make sure to replace the placeholders with your actual MongoDB connection string and default database name.

If configured correctly, you should see the following when you run `claude`:
```bash
╭───────────────────────────────────────────────────────╮
│ ✻ Welcome to Claude Code research preview!            │
│                                                       │
│   /help for help                                      │
│                                                       │
│   cwd: <path-to-project-directory>                    │
│                                                       │
│   ─────────────────────────────────────────────────── │
│                                                       │
│   MCP Servers:                                        │
│                                                       │
│   • mongo-server                            connected │
╰───────────────────────────────────────────────────────╯

```

If you run into issues, see the Claude Code [documentation](https://docs.anthropic.com/en/docs/agents-and-tools/claude-code/tutorials#set-up-model-context-protocol-mcp-servers).


## Security Considerations

- This server provides read-only access by design
- Connection strings may contain sensitive authentication information
- Store connection strings securely in environment variables
- Use a MongoDB user with read-only permissions

## Debugging

Since MCP servers communicate over stdio, debugging can be challenging. Use the [MCP Inspector](https://github.com/modelcontextprotocol/inspector), which is available as a package script:

```bash
npm run inspector
```

The Inspector will provide a URL to access debugging tools in your browser.