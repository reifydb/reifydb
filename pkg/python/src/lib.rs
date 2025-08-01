// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg(feature = "include-python-workspace")]

extern crate core;
extern crate pyo3;
extern crate pythonize;
extern crate reifydb as rdb;
extern crate serde_json;

use reifydb::variant::Embedded;
use rdb::store::Memory;
use rdb::{Principal, ReifyDB};

use pyo3::prelude::*;
use pythonize::pythonize;
use rdb::reifydb_engine::execute::ExecutionResult;
use serde_json::{Value, json};

#[pyclass(name = "Embedded")]
pub struct PyEmbedded {
    embedded: Embedded<Memory, ::reifydb_engine<Memory>>,
    root: Principal,
}

#[pymethods]
impl PyEmbedded {
    #[new]
    pub fn new() -> Self {
        let (embedded, root) = ReifyDB::embedded_blocking();
        Self { embedded, root }
    }

    pub fn tx(&self, py: Python<'_>, rql: &str) -> PyResult<PyObject> {
        let result = self
            .embedded
            .tx(&self.root, &rql)
            .into_iter()
            .map(|r| match r {
                ExecutionResult::CreateSchema { schema } => json!({
                    "type": "CreateSchema",
                    "schema": schema
                }),
                ExecutionResult::CreateTable { schema, table } => json!({
                    "type": "CreateTable",
                    "schema": schema,
                    "table": table
                }),
                ExecutionResult::InsertIntoTable { schema, table, inserted } => json!({
                    "type": "InsertIntoTable",
                    "schema": schema,
                    "table": table,
                    "inserted": inserted
                }),
                ExecutionResult::Query { columns: labels, rows } => json!({
                    "type": "Query",
                    "headers": labels.iter().map(|l| l.to_string()).collect::<Vec<_>>(),
                    "rows": rows.iter()
                        .map(|row| row.iter().map(|v| format!("{}", v)).collect::<Vec<_>>())
                        .collect::<Vec<_>>()
                }),
            })
            .collect::<Vec<Value>>();

        let array = Value::Array(result);
        Ok(pythonize(py, &array)?.into_py(py))
    }
}

#[pymodule]
fn reifydb(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEmbedded>()?;
    Ok(())
}
