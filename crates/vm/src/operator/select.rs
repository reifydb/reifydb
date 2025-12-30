// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use futures_util::StreamExt;
use reifydb_core::value::column::{Column, Columns};

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

		Box::pin(input.map(move |result| result.and_then(|batch| project_columns(&batch, &columns))))
	}
}

fn project_columns(batch: &Columns, column_names: &[String]) -> Result<Columns> {
	let mut projected: Vec<Column> = Vec::with_capacity(column_names.len());

	for name in column_names {
		let col = batch.iter().find(|c| c.name().text() == name).ok_or_else(|| VmError::ColumnNotFound {
			name: name.clone(),
		})?;

		projected.push(col.clone());
	}

	if batch.row_numbers.is_empty() {
		Ok(Columns::new(projected))
	} else {
		Ok(Columns::with_row_numbers(projected, batch.row_numbers.to_vec()))
	}
}
