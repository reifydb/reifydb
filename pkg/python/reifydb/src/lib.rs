// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg(feature = "include-python-workspace")]

extern crate core;
extern crate pyo3;
extern crate pythonize;
extern crate reifydb as rdb;
extern crate serde_json;

use pyo3::prelude::*;
use pythonize::pythonize;
use rdb::{
	Identity, ReifyDB, reifydb_engine::execute::ExecutionResult,
	store::Memory,
};
use reifydb::variant::Embedded;
use serde_json::{Value, json};

#[pyclass(name = "Embedded")]
pub struct PyEmbedded {
	embedded: Embedded<Memory, ::reifydb_engine<Memory>>,
	root: Identity,
}

#[pymethods]
impl PyEmbedded {
	#[new]
	pub fn new() -> Self {
		let (embedded, root) = ReifyDB::embedded_blocking();
		Self {
			embedded,
			root,
		}
	}

	pub fn tx(&self, py: Python<'_>, rql: &str) -> PyResult<PyObject> {
		let result = self
			.embedded
			.tx(&self.root, &rql)
			.into_iter()
			.map(|r| match r {
				ExecutionResult::CreateNamespace {
					namespace,
				} => json!({
				    "type": "CreateNamespace",
				    "namespace": namespace
				}),
				ExecutionResult::CreateTable {
					namespace,
					table,
				} => json!({
				    "type": "CreateTable",
				    "namespace": namespace,
				    "table": table
				}),
				ExecutionResult::InsertIntoTable {
					namespace,
					table,
					inserted,
				} => json!({
				    "type": "InsertIntoTable",
				    "namespace": namespace,
				    "table": table,
				    "inserted": inserted
				}),
				ExecutionResult::Query {
					columns: labels,
					rows,
				} => json!({
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
