// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::client::convert::{convert_diagnostic, convert_value};
use crate::client::grpc_db::tx_result;
use crate::client::grpc_db::tx_result::Result::{
    CreateSchema, CreateTable, DescribeQuery, Error, InsertIntoSeries, InsertIntoTable, Query,
};
use crate::client::{Client, grpc_db, wait_for_socket};
use reifydb_catalog::schema::SchemaId;
use reifydb_catalog::table::TableId;
use reifydb_core::Kind;
use reifydb_engine::{Column, CreateSchemaResult, CreateTableResult, ExecutionResult};
use std::str::FromStr;
use std::time::Duration;
use tonic::metadata::MetadataValue;

impl Client {
    pub async fn tx(&self, query: &str) -> crate::Result<Vec<ExecutionResult>> {
        // FIXME this is quite expensive and should only used in tests
        // add a server.on_ready(||{ signal_server_read() } and use it for tests instead

        wait_for_socket(&self.socket_addr, Duration::from_millis(500)).await?;
        let uri = format!("http://{}", self.socket_addr);
        let mut client = grpc_db::db_client::DbClient::connect(uri).await?;
        let mut request = tonic::Request::new(grpc_db::TxRequest { query: query.into() });

        request
            .metadata_mut()
            .insert("authorization", MetadataValue::from_str("Bearer mysecrettoken").unwrap());

        let mut stream = client.tx(request).await?.into_inner();

        let mut results = Vec::new();
        while let Some(msg) = stream.message().await? {
            if let Some(result) = msg.result {
                results.push(convert_result(result, query)?);
            }
        }
        Ok(results)
    }
}

fn convert_result(result: tx_result::Result, query: &str) -> crate::Result<ExecutionResult> {
    Ok(match result {
        Error(diagnostic) => {
            return Err(crate::Error::execution_error(query, convert_diagnostic(diagnostic)));
        }
        CreateSchema(cs) => ExecutionResult::CreateSchema(CreateSchemaResult {
            id: SchemaId(cs.id),
            schema: cs.schema,
            created: cs.created,
        }),
        CreateTable(ct) => ExecutionResult::CreateTable(CreateTableResult {
            id: TableId(ct.id),
            schema: ct.schema,
            table: ct.table,
            created: ct.created,
        }),
        InsertIntoTable(insert) => ExecutionResult::InsertIntoTable {
            schema: insert.schema,
            table: insert.table,
            inserted: insert.inserted as usize,
        },
        InsertIntoSeries(_) => unimplemented!(),
        Query(query) => {
            let labels = query
                .columns
                .into_iter()
                .map(|c| Column { name: c.name, kind: Kind::Bool })
                .collect();

            let rows = query
                .rows
                .into_iter()
                .map(|r| r.values.into_iter().map(convert_value).collect())
                .collect();

            ExecutionResult::Query { columns: labels, rows }
        }
        DescribeQuery(query) => {
            let labels = query
                .columns
                .into_iter()
                .map(|c| Column { name: c.name, kind: Kind::Bool })
                .collect();

            ExecutionResult::DescribeQuery { columns: labels }
        }
    })
}
