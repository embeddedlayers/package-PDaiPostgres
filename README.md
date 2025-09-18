# PDaiPostgres - PostgreSQL Integration for PDai

[![R Package](https://img.shields.io/badge/R-Package-blue)](https://www.r-project.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Compatible-336791)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

PDaiPostgres provides seamless PostgreSQL integration for the PDai analytics ecosystem. This package enables enterprise-grade database connectivity, optimized data processing, and real-time analytics capabilities for production environments.

## Features

- **High-Performance Connectivity**: Optimized connection pooling and query execution
- **Data Pipeline Integration**: Seamless ETL/ELT workflows with PDai analytics
- **Scalable Architecture**: Built for enterprise workloads with millions of records
- **Security First**: SSL/TLS encryption, role-based access control, and audit logging
- **Real-Time Processing**: Stream processing capabilities for live data analysis
- **Smart Caching**: Intelligent query result caching for improved performance

## Installation

### Prerequisites

- R (>= 4.0.0)
- PostgreSQL (>= 12.0)
- [PDai](https://github.com/embeddedlayers/package-PDai) base package

### From GitHub

```r
# Install devtools if not already installed
if (!require(devtools)) {
  install.packages("devtools")
}

# Install PDaiPostgres from GitHub
devtools::install_github("embeddedlayers/package-PDaiPostgres")
```

## Quick Start

```r
# Load the package
library(PDaiPostgres)

# Configure database connection
conn <- pdai_pg_connect(
  host = "your-postgres-host",
  port = 5432,
  dbname = "your-database",
  user = "your-username",
  password = "your-password",
  ssl = TRUE
)

# Execute analytics pipeline
results <- pdai_pg_pipeline(
  connection = conn,
  query = "SELECT * FROM sales_data",
  analytics = list(
    predict = TRUE,
    visualize = TRUE,
    export = "html"
  )
)

# View results
print(results$summary)
```

## Core Functions

### Connection Management
- `pdai_pg_connect()`: Establish secure database connection
- `pdai_pg_pool()`: Create connection pool for concurrent operations
- `pdai_pg_disconnect()`: Safely close connections

### Data Operations
- `pdai_pg_query()`: Execute optimized SQL queries
- `pdai_pg_stream()`: Stream large datasets efficiently
- `pdai_pg_write()`: Bulk insert/update operations
- `pdai_pg_upsert()`: Intelligent upsert operations

### Analytics Integration
- `pdai_pg_pipeline()`: Run PDai analytics on PostgreSQL data
- `pdai_pg_aggregate()`: Perform in-database aggregations
- `pdai_pg_ml()`: Execute machine learning models in-database
- `pdai_pg_cache()`: Manage analytics result caching

### Administration
- `pdai_pg_monitor()`: Monitor query performance
- `pdai_pg_optimize()`: Automatic query optimization
- `pdai_pg_audit()`: Audit trail for compliance

## Configuration

Create a `.pdai_pg_config` file in your project root:

```yaml
default:
  host: localhost
  port: 5432
  ssl: true
  pool_size: 10
  timeout: 30

production:
  host: prod-server.example.com
  port: 5432
  ssl: required
  pool_size: 50
  timeout: 60
```

## Performance Tips

1. **Use connection pooling** for applications with multiple concurrent users
2. **Enable query caching** for frequently accessed data
3. **Leverage in-database analytics** to minimize data transfer
4. **Use streaming** for large dataset processing
5. **Enable compression** for network traffic optimization

## Security Best Practices

- Always use SSL/TLS connections in production
- Implement role-based access control (RBAC)
- Use environment variables for credentials
- Enable audit logging for compliance
- Regularly update both PDaiPostgres and PostgreSQL

## Documentation

Comprehensive documentation available at:
- [Package Documentation](https://github.com/embeddedlayers/package-PDaiPostgres/wiki)
- [API Reference](https://github.com/embeddedlayers/package-PDaiPostgres/wiki/API-Reference)
- [Examples](https://github.com/embeddedlayers/package-PDaiPostgres/tree/main/examples)

## Support

- **Issues**: [GitHub Issues](https://github.com/embeddedlayers/package-PDaiPostgres/issues)
- **Email**: support@embeddedlayers.com
- **Community**: [Discussions](https://github.com/embeddedlayers/package-PDaiPostgres/discussions)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2024 PeopleDrivenAI LLC (DBA EmbeddedLayers)

## Related Projects

- [PDai](https://github.com/embeddedlayers/package-PDai): Core analytics package
- [MCP Analytics](https://github.com/embeddedlayers/mcp-analytics): Statistical analysis tools for AI assistants

---

<div align="center">
  <strong>Enterprise PostgreSQL Integration for AI-Powered Analytics</strong>
  <br>
  <sub>Part of the EmbeddedLayers Analytics Ecosystem</sub>
</div>