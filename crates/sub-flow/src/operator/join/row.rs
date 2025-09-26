use reifydb_core::{
	CowVec,
	value::row::{EncodedRow, EncodedRowNamedLayout, Row},
};
use reifydb_type::{RowNumber, Type};
use serde::{Deserialize, Serialize};

use super::Schema;

/// Serialized row for efficient storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SerializedRow {
	pub(crate) number: RowNumber,
	#[serde(with = "serde_bytes")]
	pub(crate) encoded_bytes: Vec<u8>,
}

impl SerializedRow {
	pub(crate) fn from_row(row: &Row) -> Self {
		Self {
			number: row.number,
			encoded_bytes: row.encoded.as_slice().to_vec(),
		}
	}

	pub(crate) fn to_left_row(&self, schema: &Schema) -> Row {
		// If schema is empty, we shouldn't be deserializing this row
		// This indicates a state consistency issue
		let fields: Vec<(String, Type)> = if schema.left_names.is_empty() {
			vec![]
		} else {
			schema.left_names.iter().cloned().zip(schema.left_types.iter().cloned()).collect()
		};

		let row_layout = EncodedRowNamedLayout::new(fields);
		let encoded = EncodedRow(CowVec::new(self.encoded_bytes.clone()));

		Row {
			number: self.number,
			encoded,
			layout: row_layout,
		}
	}

	pub(crate) fn to_right_row(&self, schema: &Schema) -> Row {
		// If schema is empty, we shouldn't be deserializing this row
		// This indicates a state consistency issue
		let fields: Vec<(String, Type)> = if schema.right_names.is_empty() {
			vec![]
		} else {
			schema.right_names.iter().cloned().zip(schema.right_types.iter().cloned()).collect()
		};

		let row_layout = EncodedRowNamedLayout::new(fields);
		let encoded = EncodedRow(CowVec::new(self.encoded_bytes.clone()));

		Row {
			number: self.number,
			encoded,
			layout: row_layout,
		}
	}
}
