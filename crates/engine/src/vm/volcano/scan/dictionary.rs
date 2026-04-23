// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use postcard::from_bytes;
use reifydb_core::{
	encoded::key::EncodedKey,
	interface::resolved::ResolvedDictionary,
	internal_error,
	key::{EncodableKey, dictionary::DictionaryEntryIndexKey},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, dictionary::DictionaryEntryId, r#type::Type},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct DictionaryScanNode {
	dictionary: ResolvedDictionary,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	scan_limit: Option<usize>,
}

impl DictionaryScanNode {
	pub fn new(dictionary: ResolvedDictionary, context: Arc<QueryContext>) -> Result<Self> {
		// Create column headers for dictionary scan: (id, value)
		let headers = ColumnHeaders {
			columns: vec![Fragment::internal("id"), Fragment::internal("value")],
		};

		Ok(Self {
			dictionary,
			context: Some(context),
			headers,
			last_key: None,
			exhausted: false,
			scan_limit: None,
		})
	}
}

impl QueryNode for DictionaryScanNode {
	#[instrument(name = "volcano::scan::dictionary::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		// Already has context from constructor
		Ok(())
	}

	#[instrument(name = "volcano::scan::dictionary::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "DictionaryScan::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = match self.scan_limit {
			Some(limit) => (limit as u64).min(stored_ctx.batch_size),
			None => stored_ctx.batch_size,
		};
		let dict_def = self.dictionary.def();

		// Create scan range for dictionary entries
		let range = DictionaryEntryIndexKey::full_scan(dict_def.id);

		// Collect entries for this batch
		let mut ids: Vec<DictionaryEntryId> = Vec::new();
		let mut values: Vec<Value> = Vec::new();
		let mut new_last_key = None;

		// Get entries from storage using stream
		let stream = rx.range(range, batch_size as usize)?;
		let mut count = 0;

		for entry in stream {
			let entry = entry?;

			// Skip entries we've already seen
			if let Some(ref last) = self.last_key
				&& &entry.key <= last
			{
				continue;
			}

			// Decode the key to get the entry ID
			if let Some(key) = DictionaryEntryIndexKey::decode(&entry.key) {
				// Create DictionaryEntryId with proper type
				let entry_id = DictionaryEntryId::from_u128(key.id as u128, dict_def.id_type.clone())?;

				// Decode the value from the entry
				let value: Value = from_bytes(&entry.row).map_err(|e| {
					internal_error!("Failed to deserialize dictionary value: {}", e)
				})?;

				ids.push(entry_id);
				values.push(value);
				new_last_key = Some(entry.key);

				count += 1;
				if count >= batch_size as usize {
					break;
				}
			}
		}

		if ids.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				// Empty dictionary: return empty columns with correct types to preserve shape
				let columns = Columns::new(vec![
					ColumnWithName {
						name: Fragment::internal("id"),
						data: ColumnBuffer::none_typed(dict_def.id_type.clone(), 0),
					},
					ColumnWithName {
						name: Fragment::internal("value"),
						data: ColumnBuffer::none_typed(dict_def.value_type.clone(), 0),
					},
				]);
				return Ok(Some(columns));
			}
			return Ok(None);
		}

		self.last_key = new_last_key;

		// Build columns based on dictionary types
		let id_column = build_id_column(&ids, dict_def.id_type.clone())?;
		let value_column = build_value_column(&values, dict_def.value_type.clone())?;

		let columns = Columns::new(vec![id_column, value_column]);

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}

	fn set_scan_limit(&mut self, limit: usize) {
		self.scan_limit = Some(limit);
	}
}

/// Build the ID column based on the dictionary's id_type
fn build_id_column(ids: &[DictionaryEntryId], id_type: Type) -> Result<ColumnWithName> {
	let data = match id_type {
		Type::Uint1 => {
			let vals: Vec<u8> = ids.iter().map(|id| id.to_u128() as u8).collect();
			ColumnBuffer::uint1(vals)
		}
		Type::Uint2 => {
			let vals: Vec<u16> = ids.iter().map(|id| id.to_u128() as u16).collect();
			ColumnBuffer::uint2(vals)
		}
		Type::Uint4 => {
			let vals: Vec<u32> = ids.iter().map(|id| id.to_u128() as u32).collect();
			ColumnBuffer::uint4(vals)
		}
		Type::Uint8 => {
			let vals: Vec<u64> = ids.iter().map(|id| id.to_u128() as u64).collect();
			ColumnBuffer::uint8(vals)
		}
		Type::Uint16 => {
			let vals: Vec<u128> = ids.iter().map(|id| id.to_u128()).collect();
			ColumnBuffer::uint16(vals)
		}
		_ => return Err(internal_error!("Invalid dictionary id_type: {:?}", id_type)),
	};

	Ok(ColumnWithName {
		name: Fragment::internal("id"),
		data,
	})
}

/// Build the value column based on the dictionary's value_type
fn build_value_column(values: &[Value], value_type: Type) -> Result<ColumnWithName> {
	let data = match value_type {
		Type::Utf8 => {
			let vals: Vec<String> = values
				.iter()
				.map(|v| match v {
					Value::Utf8(s) => s.clone(),
					_ => format!("{:?}", v), // Fallback representation
				})
				.collect();
			ColumnBuffer::utf8(vals)
		}
		Type::Int1 => {
			let vals: Vec<i8> = values
				.iter()
				.map(|v| match v {
					Value::Int1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int1(vals)
		}
		Type::Int2 => {
			let vals: Vec<i16> = values
				.iter()
				.map(|v| match v {
					Value::Int2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int2(vals)
		}
		Type::Int4 => {
			let vals: Vec<i32> = values
				.iter()
				.map(|v| match v {
					Value::Int4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int4(vals)
		}
		Type::Int8 => {
			let vals: Vec<i64> = values
				.iter()
				.map(|v| match v {
					Value::Int8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::int8(vals)
		}
		Type::Uint1 => {
			let vals: Vec<u8> = values
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint1(vals)
		}
		Type::Uint2 => {
			let vals: Vec<u16> = values
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint2(vals)
		}
		Type::Uint4 => {
			let vals: Vec<u32> = values
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint4(vals)
		}
		Type::Uint8 => {
			let vals: Vec<u64> = values
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnBuffer::uint8(vals)
		}
		_ => {
			// For other types, convert to string representation
			let vals: Vec<String> = values.iter().map(|v| format!("{:?}", v)).collect();
			ColumnBuffer::utf8(vals)
		}
	};

	Ok(ColumnWithName {
		name: Fragment::internal("value"),
		data,
	})
}
