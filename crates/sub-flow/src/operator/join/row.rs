use std::sync::Arc;

use reifydb_core::{
	CowVec,
	value::row::{EncodedRow, EncodedRowNamedLayout, Row},
};
use reifydb_type::{RowNumber, Type};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::Schema;

#[derive(Debug, Clone)]
pub(crate) struct SerializedRow {
	pub(crate) number: RowNumber,
	pub(crate) bytes: Arc<[u8]>,
}

impl Serialize for SerializedRow {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		use serde::ser::SerializeTuple;
		let mut tuple = serializer.serialize_tuple(2)?;
		tuple.serialize_element(&self.number)?;
		tuple.serialize_element(&self.bytes.as_ref())?;
		tuple.end()
	}
}

impl<'de> Deserialize<'de> for SerializedRow {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		use serde::de::{self, SeqAccess, Visitor};

		struct SerializedRowVisitor;

		impl<'de> Visitor<'de> for SerializedRowVisitor {
			type Value = SerializedRow;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("a tuple of (RowNumber, bytes)")
			}

			fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
			where
				A: SeqAccess<'de>,
			{
				let number = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
				let bytes: Vec<u8> =
					seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;

				Ok(SerializedRow {
					number,
					bytes: Arc::from(bytes.into_boxed_slice()),
				})
			}
		}

		deserializer.deserialize_tuple(2, SerializedRowVisitor)
	}
}

impl SerializedRow {
	pub(crate) fn from_row(row: &Row) -> Self {
		Self {
			number: row.number,
			bytes: Arc::from(row.encoded.as_slice()),
		}
	}

	pub(crate) fn to_left_row(&self, schema: &Schema) -> Row {
		debug_assert!(!schema.left_names.is_empty());

		let fields: Vec<(String, Type)> =
			schema.left_names.iter().cloned().zip(schema.left_types.iter().copied()).collect();

		let row_layout = EncodedRowNamedLayout::new(fields);

		let encoded = EncodedRow(CowVec::new(self.bytes.to_vec()));

		Row {
			number: self.number,
			encoded,
			layout: row_layout,
		}
	}

	pub(crate) fn to_right_row(&self, schema: &Schema) -> Row {
		debug_assert!(!schema.right_names.is_empty());

		let fields: Vec<(String, Type)> =
			schema.right_names.iter().cloned().zip(schema.right_types.iter().copied()).collect();

		let row_layout = EncodedRowNamedLayout::new(fields);

		let encoded = EncodedRow(CowVec::new(self.bytes.to_vec()));

		Row {
			number: self.number,
			encoded,
			layout: row_layout,
		}
	}
}
