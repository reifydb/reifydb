---
id: api
title: API Reference
sidebar_label: API Reference
sidebar_position: 1
---

# API Reference

Welcome to the ReifyDB API Reference. This section provides comprehensive documentation for all APIs and SDKs available in ReifyDB.

## Available APIs

### [Rust API](./rust)

The Rust API provides native integration with ReifyDB, offering high-performance access to all database features.

## Getting Started

To get started with ReifyDB APIs, first ensure you have ReifyDB installed. See the [Installation Guide](/docs/getting-started/installation) for more information.

## Common Operations

All ReifyDB APIs support the following core operations:

- **Connection Management**: Establish and manage connections to ReifyDB instances
- **Query Execution**: Execute SQL queries and retrieve results
- **Transaction Support**: Begin, commit, and rollback transactions
- **Schema Management**: Create and modify database schemas
- **Real-time Subscriptions**: Subscribe to real-time data changes

## Authentication

ReifyDB supports multiple authentication methods:

- **API Keys**: Simple token-based authentication
- **OAuth 2.0**: For web applications
- **mTLS**: For secure service-to-service communication

## Rate Limiting

API requests are subject to rate limiting to ensure fair usage:

- **Default Limit**: 1000 requests per minute
- **Burst Limit**: 100 requests per second
- **Custom Limits**: Available for enterprise customers

## Support

For API support and questions:

- Check our [Documentation](/docs/getting-started/installation)
- Visit our [GitHub Issues](https://github.com/reifydb/reifydb/issues)
- Join our [Community Discord](https://discord.gg/reifydb)
