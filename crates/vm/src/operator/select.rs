// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	Batch,
	value::column::{Column, Columns},
};

use crate::{
	error::{Result, VmError},
	pipeline::Pipeline,
};

/// Select operator - projects a subset of columns by name.
pub struct SelectOp {
	pub columns: Vec<String>,
}

impl SelectOp {
	pub fn new(columns: Vec<String>) -> Self {
		Self {
			columns,
		}
	}

	pub fn apply(&self, input: Pipeline) -> Pipeline {
		let columns = self.columns.clone();

		Box::new(input.map(move |result| result.and_then(|batch| project_columns(&batch, &columns))))
	}
}

fn project_columns(batch: &Batch, column_names: &[String]) -> Result<Batch> {
	// For projection, we need to materialize the batch to access column metadata
	let columns = match batch {
		Batch::Lazy(lazy) => lazy.clone().into_columns(),
		Batch::FullyMaterialized(columns) => columns.clone(),
	};

	let mut projected: Vec<Column> = Vec::with_capacity(column_names.len());

	for name in column_names {
		let col = columns.iter().find(|c| c.name().text() == name).ok_or_else(|| VmError::ColumnNotFound {
			name: name.clone(),
		})?;

		projected.push(col.clone());
	}

	let result_columns = if columns.row_numbers.is_empty() {
		Columns::new(projected)
	} else {
		Columns::with_row_numbers(projected, columns.row_numbers.to_vec())
	};

	Ok(Batch::fully_materialized(result_columns))
}
