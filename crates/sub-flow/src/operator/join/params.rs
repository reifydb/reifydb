use std::collections::HashMap;

use reifydb_core::value::row::{EncodedRow, EncodedRowNamedLayout};
use reifydb_type::{Params, Value};

/// Extended parameters that can include an encoded row
#[derive(Debug, Clone)]
pub enum RowParams {
	/// Standard parameters
	Standard(Params),
	/// Row-based parameters with named fields
	Row {
		layout: EncodedRowNamedLayout,
		row: EncodedRow,
	},
}

impl RowParams {
	/// Create parameters from an encoded row
	pub fn from_encoded_row(layout: EncodedRowNamedLayout, row: EncodedRow) -> Self {
		Self::Row {
			layout,
			row,
		}
	}

	/// Convert to standard Params by extracting all values
	pub fn to_params(&self) -> Params {
		match self {
			Self::Standard(params) => params.clone(),
			Self::Row {
				layout,
				row,
			} => {
				let mut values = HashMap::new();
				for (i, name) in layout.names().iter().enumerate() {
					let value = layout.get_value(row, i);
					values.insert(name.clone(), value);
				}
				Params::Named(values)
			}
		}
	}

	/// Get a named parameter value
	pub fn get_named(&self, name: &str) -> Option<Value> {
		match self {
			Self::Standard(params) => params.get_named(name).cloned(),
			Self::Row {
				layout,
				row,
			} => {
				// Find the field index by name
				layout.names().iter().position(|n| n == name).map(|index| layout.get_value(row, index))
			}
		}
	}
}
