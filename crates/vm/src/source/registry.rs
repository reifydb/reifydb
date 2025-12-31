// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::value::column::Columns;

use crate::{expr::ColumnSchema, pipeline::Pipeline};

/// Trait for table sources that can produce pipelines.
pub trait TableSource: Send + Sync {
	/// Get the schema of this table.
	fn schema(&self) -> Vec<ColumnSchema>;

	/// Create a scan pipeline that reads all rows from this table.
	fn scan(&self) -> Pipeline;
}

/// Registry for looking up table sources by name.
pub trait SourceRegistry {
	fn get_source(&self, name: &str) -> Option<Box<dyn TableSource>>;
}

/// In-memory table source.
pub struct InMemoryTable {
	data: Columns,
	schema: Vec<ColumnSchema>,
}

impl InMemoryTable {
	/// Create an in-memory table from a Columns batch.
	pub fn new(data: Columns) -> Self {
		let schema = data
			.iter()
			.enumerate()
			.map(|(i, col)| ColumnSchema {
				name: col.name().text().to_string(),
				index: i,
			})
			.collect();

		Self {
			data,
			schema,
		}
	}
}

impl TableSource for InMemoryTable {
	fn schema(&self) -> Vec<ColumnSchema> {
		self.schema.clone()
	}

	fn scan(&self) -> Pipeline {
		let data = self.data.clone();
		Box::pin(futures_util::stream::once(async move { Ok(data) }))
	}
}

/// In-memory source registry for testing.
pub struct InMemorySourceRegistry {
	tables: HashMap<String, InMemoryTable>,
}

impl InMemorySourceRegistry {
	/// Create an empty registry.
	pub fn new() -> Self {
		Self {
			tables: HashMap::new(),
		}
	}

	/// Register a table with the given name.
	pub fn register(&mut self, name: &str, data: Columns) {
		self.tables.insert(name.to_string(), InMemoryTable::new(data));
	}
}

impl Default for InMemorySourceRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl SourceRegistry for InMemorySourceRegistry {
	fn get_source(&self, name: &str) -> Option<Box<dyn TableSource>> {
		self.tables.get(name).map(|t| {
			Box::new(InMemoryTable {
				data: t.data.clone(),
				schema: t.schema.clone(),
			}) as Box<dyn TableSource>
		})
	}
}

// Also implement the DSL's SourceRegistry trait for use with parse_pipeline
impl crate::dsl::compile::SourceRegistry for InMemorySourceRegistry {
	fn get_source(&self, name: &str) -> Option<Box<dyn TableSource>> {
		self.tables.get(name).map(|t| {
			Box::new(InMemoryTable {
				data: t.data.clone(),
				schema: t.schema.clone(),
			}) as Box<dyn TableSource>
		})
	}
}
