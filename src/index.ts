#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import { MongoClient, Sort } from 'mongodb';

/**
 * MongoDB MCP server providing read-only access to MongoDB databases.
 * Exposes MongoDB operations as MCP tools with safety validation.
 */
class MongoDBServer {
  private server: Server;
  private client: MongoClient | null = null;
  private defaultDatabase: string | null = null;

  /**
   * Initializes the MongoDB MCP server.
   * Sets up server configuration, tools, and event handlers.
   * Uses MONGODB_URI and optional MONGODB_DEFAULT_DATABASE env vars.
   */
  constructor() {
    this.server = new Server(
      {
        name: 'mongodb-server',
        version: '0.1.0',
        description: `MongoDB MCP server providing read-only access to MongoDB databases.

Connection Requirements:
- MONGODB_URI environment variable must be set with a valid MongoDB connection string
- Optional MONGODB_DEFAULT_DATABASE environment variable for default database
- Connection string should include authentication credentials if required
- Network access to MongoDB server must be available

Best Practices:
- Start with schema discovery before complex queries
- Use limits when querying large collections
- Use projections to fetch only needed fields
- Remember all operations are read-only for safety
- Create appropriate indexes for text search and frequent queries

Error Handling:
- If a query fails, verify collection existence and schema
- Check query syntax matches MongoDB's query operators
- Ensure aggregation pipelines only use allowed stages
- Review explain plans for slow queries
- Verify text indexes exist before performing text searches`,
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    const defaultDb = process.env.MONGODB_DEFAULT_DATABASE;
    if (defaultDb) {
      this.defaultDatabase = defaultDb;
      console.error(`Using default database: ${defaultDb}`);
    }

    this.setupToolHandlers();
    
    this.server.onerror = (error) => console.error('[MCP Error]', error);
    process.on('SIGINT', async () => {
      await this.disconnect();
      await this.server.close();
      process.exit(0);
    });
  }

  /**
   * Establishes MongoDB connection using URI from environment.
   * @returns Connected MongoDB client
   * @throws {McpError} If MONGODB_URI is not configured
   */
  private async connect() {
    if (!this.client) {
      const uri = process.env.MONGODB_URI;
      if (!uri) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          'MONGODB_URI environment variable is required'
        );
      }
      this.client = new MongoClient(uri);
      await this.client.connect();
    }
    return this.client;
  }

  /**
   * Safely closes MongoDB connection.
   */
  private async disconnect() {
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }

  /**
   * Infers schema structure from document samples.
   * @param documents - Sample documents to analyze
   * @param path - Current object path for nested fields
   * @param schema - Accumulated schema object
   * @returns Inferred schema with types and examples
   */
  private async inferSchema(documents: any[], path = '', schema: any = {}) {
    for (const doc of documents) {
      Object.entries(doc).forEach(([key, value]) => {
        const fullPath = path ? `${path}.${key}` : key;
        
        if (Array.isArray(value)) {
          if (!schema[fullPath]) {
            schema[fullPath] = { type: 'array' };
          }
          
          // Handle empty arrays
          if (value.length === 0) {
            schema[fullPath].items = { type: 'unknown' };
          } 
          // Handle arrays of primitives
          else if (typeof value[0] !== 'object' || value[0] === null) {
            schema[fullPath].items = { type: typeof value[0] };
          }
          // Handle arrays of objects
          else {
            schema[fullPath].items = { type: 'object', properties: {} };
            this.inferSchema(value, `${fullPath}.items.properties`, schema);
          }
        } else if (value && typeof value === 'object') {
          if (!schema[fullPath]) {
            schema[fullPath] = { type: 'object', properties: {} };
          }
          this.inferSchema([value], `${fullPath}.properties`, schema);
        } else {
          if (!schema[fullPath]) {
            schema[fullPath] = { 
              type: typeof value,
              example: value, // Add example value for better understanding
              nullable: value === null
            };
          }
        }
      });
    }
    return schema;
  }

  /**
   * Validates aggregation pipeline for safety.
   * Prevents stages that could modify data.
   * @param pipeline - MongoDB aggregation pipeline
   * @throws {McpError} If pipeline contains unsafe operations
   */
  private validateAggregationPipeline(pipeline: any[]): void {
    if (!Array.isArray(pipeline)) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'Aggregation pipeline must be an array'
      );
    }

    if (pipeline.length === 0) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'Aggregation pipeline cannot be empty'
      );
    }

    const unsafeStages = ['$out', '$merge', '$addFields', '$set', '$unset', '$replaceRoot', '$replaceWith'];
    const unsafeStageFound = pipeline.find(stage => 
      Object.keys(stage).some(key => unsafeStages.includes(key))
    );

    if (unsafeStageFound) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'Pipeline contains unsafe stages that could modify data. Only read-only operations are allowed.'
      );
    }
  }

  /**
   * Generates visualization suggestions based on result data.
   * @param data - Query results to analyze
   * @returns Recommended visualization approaches based on data characteristics
   */
  private generateVisualizationHint(data: any[]): string {
    if (!Array.isArray(data) || data.length === 0) return '';

    // Check if the data looks like time series
    const hasDateFields = Object.keys(data[0]).some(key => 
      data[0][key] instanceof Date || 
      (typeof data[0][key] === 'string' && !isNaN(Date.parse(data[0][key])))
    );

    // Check if the data has numeric fields
    const numericFields = Object.keys(data[0]).filter(key => 
      typeof data[0][key] === 'number'
    );

    // Check if the data has categorical fields
    const categoricalFields = Object.keys(data[0]).filter(key => 
      typeof data[0][key] === 'string' && 
      data.every(item => typeof item[key] === 'string')
    );

    // Check if the data has geospatial fields
    const hasGeoData = Object.keys(data[0]).some(key => {
      const value = data[0][key];
      return value && typeof value === 'object' && 
        (('type' in value && value.type === 'Point' && 'coordinates' in value) ||
         (Array.isArray(value) && value.length === 2 && 
          typeof value[0] === 'number' && typeof value[1] === 'number'));
    });

    let hints = [];

    if (hasDateFields && numericFields.length > 0) {
      hints.push('Time Series Visualization:\n- Consider line charts for temporal trends\n- Time-based heat maps for density patterns\n- Area charts for cumulative values over time');
    }

    if (categoricalFields.length > 0 && numericFields.length > 0) {
      hints.push('Categorical Analysis:\n- Bar charts for comparing categories\n- Box plots for distribution analysis\n- Heat maps for category correlations\n- Treemaps for hierarchical data');
    }

    if (numericFields.length >= 2) {
      hints.push('Numerical Analysis:\n- Scatter plots for correlation analysis\n- Bubble charts if three numeric dimensions\n- Correlation matrices for multiple variables\n- Histograms for distribution analysis');
    }

    if (hasGeoData) {
      hints.push('Geospatial Visualization:\n- Map overlays for location data\n- Choropleth maps for regional analysis\n- Heat maps for density visualization\n- Cluster maps for point concentration');
    }

    if (data.length > 1000) {
      hints.push('Large Dataset Considerations:\n- Consider sampling for initial visualization\n- Use aggregation for summary views\n- Implement pagination or infinite scroll\n- Consider server-side rendering');
    }

    return hints.join('\n\n');
  }
  
  /**
   * Converts MongoDB documents to CSV format.
   * @param docs - MongoDB documents to convert
   * @param options - Format options like header inclusion, delimiter
   * @returns CSV formatted string
   */
  private documentsToCsv(docs: any[], options: {
    includeHeaders?: boolean;
    delimiter?: string;
  } = {}): string {
    if (!Array.isArray(docs) || docs.length === 0) return '';
    
    const delimiter = options.delimiter || ',';
    const includeHeaders = options.includeHeaders !== false;
    
    // Extract all possible field names from all documents (handles varying schemas)
    const fieldsSet = new Set<string>();
    docs.forEach(doc => {
      Object.keys(doc).forEach(key => fieldsSet.add(key));
    });
    
    const fields = Array.from(fieldsSet);
    let result = '';
    
    // Add headers
    if (includeHeaders) {
      result += fields.map(field => this.escapeCsvField(field, delimiter)).join(delimiter) + '\n';
    }
    
    // Add data rows
    docs.forEach(doc => {
      const row = fields.map(field => {
        const value = doc[field];
        if (value === undefined || value === null) return '';
        if (typeof value === 'object') return this.escapeCsvField(JSON.stringify(value), delimiter);
        return this.escapeCsvField(String(value), delimiter);
      });
      result += row.join(delimiter) + '\n';
    });
    
    return result;
  }

  /**
   * Escapes a field for CSV format.
   * @param field - Field value to escape
   * @param delimiter - CSV delimiter character
   * @returns Escaped field value
   */
  private escapeCsvField(field: string, delimiter: string): string {
    // If field contains delimiter, newline, or quotes, wrap in quotes and escape internal quotes
    if (field.includes(delimiter) || field.includes('\n') || field.includes('"')) {
      return `"${field.replace(/"/g, '""')}"`;
    }
    return field;
  }

  /**
   * Registers all MongoDB tool handlers with the MCP server.
   * Sets up request handlers for listing available tools and executing them.
   */
  private setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: [
        {
          name: 'list_databases',
          description: 'List all databases in the MongoDB server.',
          inputSchema: {
            type: 'object',
            properties: {},
          },
        },
        {
          name: 'list_collections',
          description: `List all collections in a database.
          
Start here to understand what collections are available before querying.`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
            },
          },
        },
        {
          name: 'get_schema',
          description: `Infer schema from a collection by analyzing sample documents.
        
Best Practice: Use this before querying to understand collection structure.

Example:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "get_schema",
  arguments: {
    "collection": "users",
    "sampleSize": 100
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              sampleSize: {
                type: 'number',
                description: 'Number of documents to sample (default: 100)',
                minimum: 1,
                maximum: 1000,
              },
            },
            required: ['collection'],
          },
        },
        {
          name: 'query',
          description: `Execute a read-only query on a collection using MongoDB query syntax.

Supports both JSON and CSV output formats:
- Use outputFormat="json" for standard JSON (default)
- Use outputFormat="csv" for comma-separated values export

Best Practices:
- Use projections to fetch only needed fields
- Add limits for large collections
- Use sort for consistent ordering

Example - Standard Query:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "query",
  arguments: {
    "collection": "users",
    "filter": { "age": { "$gte": 21 } },
    "projection": { "name": 1, "email": 1 },
    "sort": { "name": 1 },
    "limit": 100
  }

Example - CSV Export:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "query",
  arguments: {
    "collection": "users",
    "filter": { "active": true },
    "outputFormat": "csv",
    "formatOptions": {
      "includeHeaders": true,
      "delimiter": ","
    }
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              filter: {
                type: 'object',
                description: 'MongoDB query filter using standard MongoDB operators ($eq, $gt, $in, etc.)',
              },
              projection: {
                type: 'object',
                description: 'MongoDB projection to specify fields to return (optional)',
              },
              sort: {
                type: 'object',
                description: 'MongoDB sort specification (optional)',
              },
              limit: {
                type: 'number',
                description: 'Maximum number of documents to return (optional)',
                minimum: 1,
                maximum: 1000,
              },
              outputFormat: {
                type: 'string',
                description: 'Output format for results (json or csv)',
                enum: ['json', 'csv'],
              },
              formatOptions: {
                type: 'object',
                description: 'Format-specific options',
                properties: {
                  delimiter: {
                    type: 'string',
                    description: 'CSV delimiter character (default: comma)',
                  },
                  includeHeaders: {
                    type: 'boolean',
                    description: 'Whether to include header row in CSV (default: true)',
                  },
                },
              },
            },
            required: ['collection', 'filter'],
          },
        },
        {
          name: 'aggregate',
          description: `Execute a read-only aggregation pipeline on a collection.

Supported Stages:
- $match: Filter documents
- $group: Group documents by a key
- $sort: Sort documents
- $project: Shape the output
- $lookup: Perform left outer joins
- $unwind: Deconstruct array fields

Unsafe/Blocked Stages:
- $out: Write results to collection
- $merge: Merge results into collection
- $addFields: Add new fields
- $set: Set field values
- $unset: Remove fields
- $replaceRoot: Replace document structure
- $replaceWith: Replace document

Example - User Statistics by Role:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "aggregate",
  arguments: {
    "collection": "users",
    "pipeline": [
      { "$match": { "active": true } },
      { "$group": {
          "_id": "$role",
          "count": { "$sum": 1 },
          "avgAge": { "$avg": "$age" }
      }},
      { "$sort": { "count": -1 } }
    ],
    "limit": 100
  }

Example - Posts with Author Details:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "aggregate",
  arguments: {
    "collection": "posts",
    "pipeline": [
      { "$match": { "published": true } },
      { "$lookup": {
          "from": "users",
          "localField": "authorId",
          "foreignField": "_id",
          "as": "author"
      }},
      { "$unwind": "$author" },
      { "$project": {
          "title": 1,
          "authorName": "$author.name",
          "publishDate": 1
      }}
    ]
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              pipeline: {
                type: 'array',
                description: 'MongoDB aggregation pipeline stages (read-only operations only)',
                items: {
                  type: 'object',
                },
              },
              limit: {
                type: 'number',
                description: 'Maximum number of documents to return (optional)',
                minimum: 1,
                maximum: 1000,
              },
            },
            required: ['collection', 'pipeline'],
          },
        },
        {
          name: 'get_collection_stats',
          description: `Get detailed statistics about a collection.

Returns information about:
- Document count and size
- Storage metrics
- Index sizes and usage
- Average document size
- Padding factor`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
            },
            required: ['collection'],
          },
        },
        {
          name: 'get_indexes',
          description: `Get information about indexes on a collection.

Returns details about:
- Index names and fields
- Index types (single field, compound, text, etc.)
- Index sizes
- Index options
- Usage statistics`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
            },
            required: ['collection'],
          },
        },
        {
          name: 'explain_query',
          description: `Get the execution plan for a query.

Helps understand:
- How MongoDB will execute the query
- Which indexes will be used
- Number of documents examined
- Execution stages and timing

Use this to optimize slow queries.`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              filter: {
                type: 'object',
                description: 'MongoDB query filter to explain',
              },
              projection: {
                type: 'object',
                description: 'MongoDB projection (optional)',
              },
              sort: {
                type: 'object',
                description: 'MongoDB sort specification (optional)',
              },
            },
            required: ['collection', 'filter'],
          },
        },
        {
          name: 'get_distinct_values',
          description: `Get distinct values for a field in a collection.

Useful for:
- Understanding data distribution
- Finding unique categories
- Data quality checks
- Identifying outliers

Example:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "get_distinct_values",
  arguments: {
    "collection": "users",
    "field": "role",
    "filter": { "active": true }
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              field: {
                type: 'string',
                description: 'Field name to get distinct values for',
              },
              filter: {
                type: 'object',
                description: 'MongoDB query filter to apply before getting distinct values (optional)',
              },
            },
            required: ['collection', 'field'],
          },
        },
        {
          name: 'sample_data',
          description: `Get a random sample of documents from a collection.
  
Supports both JSON and CSV output formats:
- Use outputFormat="json" for standard JSON (default)
- Use outputFormat="csv" for comma-separated values export

Useful for:
- Exploratory data analysis
- Testing with representative data
- Understanding data distribution
- Performance testing with realistic data subsets

Example - JSON Sample:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "sample_data",
  arguments: {
    "collection": "users",
    "size": 50
  }

Example - CSV Export:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "sample_data",
  arguments: {
    "collection": "users",
    "size": 100,
    "outputFormat": "csv",
    "formatOptions": {
      "includeHeaders": true,
      "delimiter": ","
    }
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              size: {
                type: 'number',
                description: 'Number of random documents to sample (default: 10)',
                minimum: 1,
                maximum: 1000,
              },
              outputFormat: {
                type: 'string',
                description: 'Output format for results (json or csv)',
                enum: ['json', 'csv'],
              },
              formatOptions: {
                type: 'object',
                description: 'Format-specific options',
                properties: {
                  delimiter: {
                    type: 'string',
                    description: 'CSV delimiter character (default: comma)',
                  },
                  includeHeaders: {
                    type: 'boolean',
                    description: 'Whether to include header row in CSV (default: true)',
                  },
                },
              },
            },
            required: ['collection'],
          },
        },
        {
          name: 'count_documents',
          description: `Count documents in a collection that match a filter.
  
Benefits:
- More efficient than retrieving full documents
- Good for understanding data volume
- Can help planning query strategies
- Optimize pagination implementation

Example:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "count_documents",
  arguments: {
    "collection": "users",
    "filter": { "active": true, "age": { "$gte": 21 } }
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              filter: {
                type: 'object',
                description: 'MongoDB query filter (optional, defaults to count all documents)',
              },
            },
            required: ['collection'],
          },
        },
        {
          name: 'find_by_ids',
          description: `Find multiple documents by their IDs in a single request.
  
Advantages:
- More efficient than multiple single document lookups
- Preserves ID order in results when possible
- Can filter specific fields with projection
- Handles both string and ObjectId identifiers

Example:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "find_by_ids",
  arguments: {
    "collection": "products",
    "ids": ["5f8d0f3c", "5f8d0f3d", "5f8d0f3e"],
    "idField": "_id",
    "projection": { "name": 1, "price": 1 }
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              ids: {
                type: 'array',
                description: 'Array of document IDs to look up',
                items: {
                  type: ['string', 'number'],
                },
              },
              idField: {
                type: 'string',
                description: 'Field containing the IDs (default: "_id")',
              },
              projection: {
                type: 'object',
                description: 'MongoDB projection to specify fields to return (optional)',
              },
            },
            required: ['collection', 'ids'],
          },
        },
        {
          name: 'geo_query',
          description: `Execute geospatial queries on a MongoDB collection.

Supports:
- Finding points near a location
- Finding documents within a polygon, circle, or box
- Calculating distances between points
- GeoJSON and legacy coordinate pair formats

Requirements:
- Collection must have a geospatial index (2dsphere recommended)
- Coordinates should follow MongoDB conventions (longitude first, then latitude)

Examples:
1. Find locations near a point (2 miles radius):
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "geo_query",
  arguments: {
    "collection": "restaurants",
    "operation": "near",
    "point": [-73.9667, 40.78],
    "maxDistance": 3218.69,  // 2 miles in meters
    "distanceField": "distance"
  }

2. Find locations within a polygon:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "geo_query",
  arguments: {
    "collection": "properties",
    "operation": "geoWithin",
    "geometry": {
      "type": "Polygon",
      "coordinates": [
        [[-73.958, 40.8], [-73.94, 40.79], [-73.95, 40.76], [-73.97, 40.76], [-73.958, 40.8]]
      ]
    }
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              operation: {
                type: 'string',
                description: 'Geospatial operation to perform',
                enum: ['near', 'geoWithin', 'geoIntersects', 'nearSphere'],
              },
              locationField: {
                type: 'string',
                description: 'Field containing geospatial data (default: "location")',
              },
              point: {
                type: 'array',
                description: 'Point coordinates [longitude, latitude] for near/nearSphere queries',
                items: {
                  type: 'number',
                },
              },
              maxDistance: {
                type: 'number',
                description: 'Maximum distance in meters for near/nearSphere queries',
              },
              minDistance: {
                type: 'number',
                description: 'Minimum distance in meters for near/nearSphere queries',
              },
              geometry: {
                type: 'object',
                description: 'GeoJSON geometry for geoWithin/geoIntersects queries',
              },
              distanceField: {
                type: 'string',
                description: 'Field to store calculated distances (for near/nearSphere queries)',
              },
              spherical: {
                type: 'boolean',
                description: 'Calculate distances on a sphere (Earth) rather than flat plane',
              },
              limit: {
                type: 'number',
                description: 'Maximum number of results to return',
                minimum: 1,
                maximum: 1000,
              },
              additionalFilter: {
                type: 'object',
                description: 'Additional MongoDB query criteria to combine with geospatial query',
              },
            },
            required: ['collection', 'operation'],
          },
        },
        {
          name: 'text_search',
          description: `Perform a full-text search on a collection.

Requirements:
- Collection must have a text index
- Only one text index per collection is allowed

Features:
- Supports phrases and keywords
- Word stemming
- Stop words removal
- Text score ranking

Example:
use_mcp_tool with
  server_name: "mongodb",
  tool_name: "text_search",
  arguments: {
    "collection": "articles",
    "searchText": "mongodb database",
    "filter": { "published": true },
    "limit": 10,
    "includeScore": true
  }`,
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Database name (optional if default database is configured)',
              },
              collection: {
                type: 'string',
                description: 'Collection name',
              },
              searchText: {
                type: 'string',
                description: 'Text to search for',
              },
              filter: {
                type: 'object',
                description: 'Additional MongoDB query filter (optional)',
              },
              limit: {
                type: 'number',
                description: 'Maximum number of results to return (optional)',
                minimum: 1,
                maximum: 1000,
              },
              includeScore: {
                type: 'boolean',
                description: 'Include text search score in results (optional)',
              },
            },
            required: ['collection', 'searchText'],
          },
        },
      ],
    }));

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const client = await this.connect();

      try {
        switch (request.params.name) {
          case 'list_databases': {
            const adminDb = client.db('admin');
            const result = await adminDb.admin().listDatabases();
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(result.databases, null, 2),
                },
              ],
            };
          }

          case 'list_collections': {
            const { database } = request.params.arguments as { database?: string };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }
            const db = client.db(dbName);
            const collections = await db.listCollections().toArray();
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(collections, null, 2),
                },
              ],
            };
          }

          case 'get_schema': {
            const { database, collection, sampleSize = 100 } = request.params.arguments as {
              database?: string;
              collection: string;
              sampleSize?: number;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }
            const db = client.db(dbName);
            const docs = await db
              .collection(collection)
              .find()
              .limit(sampleSize)
              .toArray();
            
            if (docs.length === 0) {
              return {
                content: [
                  {
                    type: 'text',
                    text: 'No documents found in collection',
                  },
                ],
              };
            }

            const schema = await this.inferSchema(docs);
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(schema, null, 2),
                },
              ],
            };
          }

          case 'query': {
            const { 
              database, 
              collection, 
              filter, 
              projection, 
              sort, 
              limit,
              outputFormat = 'json',
              formatOptions = {} 
            } = request.params.arguments as {
              database?: string;
              collection: string;
              filter: object;
              projection?: object;
              sort?: Sort;
              limit?: number;
              outputFormat?: 'json' | 'csv';
              formatOptions?: any;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }

            const db = client.db(dbName);
            let query = db.collection(collection).find(filter);

            if (projection) {
              query = query.project(projection);
            }
            if (sort) {
              query = query.sort(sort);
            }
            if (limit) {
              query = query.limit(limit);
            }

            const results = await query.toArray();
            
            // Handle different output formats
            if (outputFormat.toLowerCase() === 'csv') {
              return {
                content: [
                  {
                    type: 'text',
                    text: this.documentsToCsv(results, formatOptions),
                  },
                ],
              };
            } else {
              // Default JSON format
              const vizHint = this.generateVisualizationHint(results);
              return {
                content: [
                  {
                    type: 'text',
                    text: JSON.stringify(results, null, 2) + (vizHint ? `\n\nVisualization Hint:\n${vizHint}` : ''),
                  },
                ],
              };
            }
          }

          case 'aggregate': {
            const { database, collection, pipeline, limit } = request.params.arguments as {
              database?: string;
              collection: string;
              pipeline: any[];
              limit?: number;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }

            this.validateAggregationPipeline(pipeline);

            const db = client.db(dbName);
            let aggregation = db.collection(collection).aggregate(pipeline);

            if (limit) {
              aggregation = aggregation.limit(limit);
            }

            const results = await aggregation.toArray();
            const vizHint = this.generateVisualizationHint(results);

            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(results, null, 2) + (vizHint ? `\n\nVisualization Hint:\n${vizHint}` : ''),
                },
              ],
            };
          }

          case 'get_collection_stats': {
            const { database, collection } = request.params.arguments as {
              database?: string;
              collection: string;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }

            const db = client.db(dbName);
            const stats = await db.command({ collStats: collection });
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(stats, null, 2),
                },
              ],
            };
          }

          case 'get_indexes': {
            const { database, collection } = request.params.arguments as {
              database?: string;
              collection: string;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }

            const db = client.db(dbName);
            const indexes = await db.collection(collection).indexes();
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(indexes, null, 2),
                },
              ],
            };
          }

          case 'explain_query': {
            const { database, collection, filter, projection, sort } = request.params
              .arguments as {
              database?: string;
              collection: string;
              filter: object;
              projection?: object;
              sort?: Sort;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }

            const db = client.db(dbName);
            let query = db.collection(collection).find(filter);

            if (projection) {
              query = query.project(projection);
            }
            if (sort) {
              query = query.sort(sort);
            }

            const explanation = await query.explain();
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(explanation, null, 2),
                },
              ],
            };
          }

          case 'get_distinct_values': {
            const { database, collection, field, filter } = request.params.arguments as {
              database?: string;
              collection: string;
              field: string;
              filter?: object;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }

            const db = client.db(dbName);
            const values = await db.collection(collection).distinct(field, filter || {});
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(values, null, 2),
                },
              ],
            };
          }

          case 'sample_data': {
            const { 
              database, 
              collection, 
              size = 10,
              outputFormat = 'json',
              formatOptions = {}
            } = request.params.arguments as {
              database?: string;
              collection: string;
              size?: number;
              outputFormat?: 'json' | 'csv';
              formatOptions?: any;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }
            
            const db = client.db(dbName);
            const sampleSize = Math.min(size, 1000); // Cap sample size for safety
            
            const results = await db.collection(collection).aggregate([
              { $sample: { size: sampleSize } }
            ]).toArray();
            
            // Handle different output formats
            if (outputFormat.toLowerCase() === 'csv') {
              return {
                content: [
                  {
                    type: 'text',
                    text: this.documentsToCsv(results, formatOptions),
                  },
                ],
              };
            } else {
              // Default JSON format
              const vizHint = this.generateVisualizationHint(results);
              return {
                content: [
                  {
                    type: 'text',
                    text: JSON.stringify(results, null, 2) + (vizHint ? `\n\nVisualization Hint:\n${vizHint}` : ''),
                  },
                ],
              };
            }
          }

          case 'count_documents': {
            const { database, collection, filter = {} } = request.params.arguments as {
              database?: string;
              collection: string;
              filter?: object;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }
            
            const db = client.db(dbName);
            const count = await db.collection(collection).countDocuments(filter);
            
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify({ count }, null, 2),
                },
              ],
            };
          }

          case 'find_by_ids': {
            const { database, collection, ids, idField = '_id', projection } = request.params.arguments as {
              database?: string;
              collection: string;
              ids: (string | number)[];
              idField?: string;
              projection?: object;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }
            
            if (!Array.isArray(ids) || ids.length === 0) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'The ids parameter must be a non-empty array'
              );
            }
            
            const db = client.db(dbName);
            let query = db.collection(collection).find({ [idField]: { $in: ids } });
            
            if (projection) {
              query = query.project(projection);
            }
            
            const results = await query.toArray();
            
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(results, null, 2),
                },
              ],
            };
          }

          case 'geo_query': {
            const { 
              database, 
              collection, 
              operation, 
              locationField = 'location',
              point, 
              maxDistance, 
              minDistance, 
              geometry, 
              distanceField,
              spherical = true, 
              limit = 100, 
              additionalFilter = {} 
            } = request.params.arguments as {
              database?: string;
              collection: string;
              operation: 'near' | 'geoWithin' | 'geoIntersects' | 'nearSphere';
              locationField?: string;
              point?: number[];
              maxDistance?: number;
              minDistance?: number;
              geometry?: any;
              distanceField?: string;
              spherical?: boolean;
              limit?: number;
              additionalFilter?: object;
            };
            
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }

            const db = client.db(dbName);
            
            switch (operation) {
              case 'near':
              case 'nearSphere': {
                if (!Array.isArray(point) || point.length !== 2) {
                  throw new McpError(
                    ErrorCode.InvalidRequest,
                    'Point coordinates [longitude, latitude] are required for near/nearSphere queries'
                  );
                }
                
                // Validate coordinates
                const [longitude, latitude] = point;
                if (longitude < -180 || longitude > 180) {
                  throw new McpError(
                    ErrorCode.InvalidRequest,
                    'Invalid longitude: must be between -180 and 180'
                  );
                }
                if (latitude < -90 || latitude > 90) {
                  throw new McpError(
                    ErrorCode.InvalidRequest,
                    'Invalid latitude: must be between -90 and 90'
                  );
                }
                
                const geoNearOptions: any = {
                  near: { type: 'Point', coordinates: point },
                  distanceField: distanceField || 'distance',
                  spherical: operation === 'nearSphere' || spherical,
                  query: additionalFilter
                };
                
                if (maxDistance !== undefined) geoNearOptions.maxDistance = maxDistance;
                if (minDistance !== undefined) geoNearOptions.minDistance = minDistance;
                if (limit) geoNearOptions.limit = limit;
                
                try {
                  // Use aggregation for geoNear
                  const results = await db.collection(collection).aggregate([
                    { $geoNear: geoNearOptions }
                  ]).toArray();
                  
                  const vizHint = this.generateVisualizationHint(results);
                  
                  return {
                    content: [
                      {
                        type: 'text',
                        text: JSON.stringify(results, null, 2) + 
                              (vizHint ? `\n\nVisualization Hint:\n${vizHint}` : ''),
                      },
                    ],
                  };
                } catch (error) {
                  // Check if error is due to missing geospatial index
                  if (error instanceof Error && 
                      (error.message.includes('2dsphere') || error.message.includes('geo'))) {
                    throw new McpError(
                      ErrorCode.InvalidRequest,
                      `Geospatial index required. Create one using: db.${collection}.createIndex({ "${locationField}": "2dsphere" })`
                    );
                  }
                  throw error;
                }
              }
              
              case 'geoWithin':
              case 'geoIntersects': {
                if (!geometry || !geometry.type || !geometry.coordinates) {
                  throw new McpError(
                    ErrorCode.InvalidRequest,
                    'Valid GeoJSON geometry is required for geoWithin/geoIntersects queries'
                  );
                }
                
                const operator = operation === 'geoWithin' ? '$geoWithin' : '$geoIntersects';
                
                const geoQuery = {
                  [locationField]: {
                    [operator]: {
                      $geometry: geometry
                    }
                  },
                  ...additionalFilter
                };
                
                try {
                  const results = await db.collection(collection)
                    .find(geoQuery)
                    .limit(limit)
                    .toArray();
                  
                  const vizHint = this.generateVisualizationHint(results);
                  
                  return {
                    content: [
                      {
                        type: 'text',
                        text: JSON.stringify(results, null, 2) + 
                              (vizHint ? `\n\nVisualization Hint:\n${vizHint}` : ''),
                      },
                    ],
                  };
                } catch (error) {
                  // Check if error is due to missing geospatial index
                  if (error instanceof Error && 
                      (error.message.includes('2dsphere') || error.message.includes('geo'))) {
                    throw new McpError(
                      ErrorCode.InvalidRequest,
                      `Geospatial index required. Create one using: db.${collection}.createIndex({ "${locationField}": "2dsphere" })`
                    );
                  }
                  throw error;
                }
              }
              
              default:
                throw new McpError(
                  ErrorCode.InvalidRequest,
                  `Unsupported geospatial operation: ${operation}`
                );
            }
          }

          case 'text_search': {
            const { database, collection, searchText, filter, limit, includeScore } = request.params
              .arguments as {
              database?: string;
              collection: string;
              searchText: string;
              filter?: object;
              limit?: number;
              includeScore?: boolean;
            };
            const dbName = database || this.defaultDatabase;
            if (!dbName) {
              throw new McpError(
                ErrorCode.InvalidRequest,
                'Database name is required when no default database is configured'
              );
            }

            const db = client.db(dbName);
            
            try {
              const searchQuery = {
                $text: { $search: searchText },
                ...(filter || {}),
              };

              const projection = includeScore ? { score: { $meta: 'textScore' } } : undefined;
              let query = db.collection(collection).find(searchQuery);

              if (projection) {
                query = query.project(projection);
              }
              if (includeScore) {
                query = query.sort({ score: { $meta: 'textScore' } });
              }
              if (limit) {
                query = query.limit(limit);
              }

              const results = await query.toArray();
              return {
                content: [
                  {
                    type: 'text',
                    text: JSON.stringify(results, null, 2),
                  },
                ],
              };
            } catch (error) {
              // Check if error is due to missing text index
              if (error instanceof Error && error.message.includes('text index')) {
                throw new McpError(
                  ErrorCode.InvalidRequest,
                  'No text index found on this collection. Create a text index first using db.collection.createIndex({ "field": "text" })'
                );
              }
              throw error;
            }
          }

          default:
            throw new McpError(
              ErrorCode.MethodNotFound,
              `Unknown tool: ${request.params.name}`
            );
        }
      } catch (error) {
        if (error instanceof McpError) {
          throw error;
        }
        throw new McpError(
          ErrorCode.InternalError,
          `MongoDB error: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    });
  }

  /**
   * Starts the MCP server with stdio transport.
   * Handles incoming MCP requests until terminated.
   */
  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('MongoDB MCP server running on stdio');
  }
}

const server = new MongoDBServer();
server.run().catch(console.error);
