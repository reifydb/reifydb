// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	EncodedKey,
	interface::{EncodableKey, resolved::ResolvedDictionary},
	key::DictionaryEntryIndexKey,
	value::column::{Column, ColumnData, Columns, headers::ColumnHeaders},
};
use reifydb_type::{DictionaryEntryId, Fragment, Type, Value, internal_error};
use tracing::instrument;

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
};

pub struct DictionaryScanNode {
	dictionary: ResolvedDictionary,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl DictionaryScanNode {
	pub fn new(dictionary: ResolvedDictionary, context: Arc<ExecutionContext>) -> crate::Result<Self> {
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
		})
	}
}

#[async_trait]
impl QueryNode for DictionaryScanNode {
	#[instrument(name = "query::scan::dictionary::initialize", level = "trace", skip_all)]
	async fn initialize<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		_ctx: &ExecutionContext,
	) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	#[instrument(name = "query::scan::dictionary::next", level = "trace", skip_all)]
	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "DictionaryScan::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let dict_def = self.dictionary.def();

		// Create scan range for dictionary entries
		let range = DictionaryEntryIndexKey::full_scan(dict_def.id);

		// Collect entries for this batch
		let mut ids: Vec<DictionaryEntryId> = Vec::new();
		let mut values: Vec<Value> = Vec::new();
		let mut new_last_key = None;

		// Get entries from storage
		let entries: Vec<_> = rx
			.range_batch(range, batch_size).await?
			.items.into_iter()
			// Skip entries we've already seen
			.skip_while(|entry| {
				if let Some(ref last) = self.last_key {
					&entry.key <= last
				} else {
					false
				}
			})
			.take(batch_size as usize)
			.collect();

		for entry in entries {
			// Decode the key to get the entry ID
			if let Some(key) = DictionaryEntryIndexKey::decode(&entry.key) {
				// Create DictionaryEntryId with proper type
				let entry_id = DictionaryEntryId::from_u128(key.id as u128, dict_def.id_type)?;

				// Decode the value from the entry
				let (value, _): (Value, _) =
					bincode::serde::decode_from_slice(&entry.values, bincode::config::standard())
						.map_err(|e| {
						internal_error!("Failed to deserialize dictionary value: {}", e)
					})?;

				ids.push(entry_id);
				values.push(value);
				new_last_key = Some(entry.key);
			}
		}

		if ids.is_empty() {
			self.exhausted = true;
			return Ok(None);
		}

		self.last_key = new_last_key;

		// Build columns based on dictionary types
		let id_column = build_id_column(&ids, dict_def.id_type)?;
		let value_column = build_value_column(&values, dict_def.value_type)?;

		let columns = Columns::new(vec![id_column, value_column]);

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

/// Build the ID column based on the dictionary's id_type
fn build_id_column(ids: &[DictionaryEntryId], id_type: Type) -> crate::Result<Column> {
	let data = match id_type {
		Type::Uint1 => {
			let vals: Vec<u8> = ids.iter().map(|id| id.to_u128() as u8).collect();
			ColumnData::uint1(vals)
		}
		Type::Uint2 => {
			let vals: Vec<u16> = ids.iter().map(|id| id.to_u128() as u16).collect();
			ColumnData::uint2(vals)
		}
		Type::Uint4 => {
			let vals: Vec<u32> = ids.iter().map(|id| id.to_u128() as u32).collect();
			ColumnData::uint4(vals)
		}
		Type::Uint8 => {
			let vals: Vec<u64> = ids.iter().map(|id| id.to_u128() as u64).collect();
			ColumnData::uint8(vals)
		}
		Type::Uint16 => {
			let vals: Vec<u128> = ids.iter().map(|id| id.to_u128()).collect();
			ColumnData::uint16(vals)
		}
		_ => return Err(internal_error!("Invalid dictionary id_type: {:?}", id_type).into()),
	};

	Ok(Column {
		name: Fragment::internal("id"),
		data,
	})
}

/// Build the value column based on the dictionary's value_type
fn build_value_column(values: &[Value], value_type: Type) -> crate::Result<Column> {
	let data = match value_type {
		Type::Utf8 => {
			let vals: Vec<String> = values
				.iter()
				.map(|v| match v {
					Value::Utf8(s) => s.clone(),
					_ => format!("{:?}", v), // Fallback representation
				})
				.collect();
			ColumnData::utf8(vals)
		}
		Type::Int1 => {
			let vals: Vec<i8> = values
				.iter()
				.map(|v| match v {
					Value::Int1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int1(vals)
		}
		Type::Int2 => {
			let vals: Vec<i16> = values
				.iter()
				.map(|v| match v {
					Value::Int2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int2(vals)
		}
		Type::Int4 => {
			let vals: Vec<i32> = values
				.iter()
				.map(|v| match v {
					Value::Int4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int4(vals)
		}
		Type::Int8 => {
			let vals: Vec<i64> = values
				.iter()
				.map(|v| match v {
					Value::Int8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::int8(vals)
		}
		Type::Uint1 => {
			let vals: Vec<u8> = values
				.iter()
				.map(|v| match v {
					Value::Uint1(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint1(vals)
		}
		Type::Uint2 => {
			let vals: Vec<u16> = values
				.iter()
				.map(|v| match v {
					Value::Uint2(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint2(vals)
		}
		Type::Uint4 => {
			let vals: Vec<u32> = values
				.iter()
				.map(|v| match v {
					Value::Uint4(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint4(vals)
		}
		Type::Uint8 => {
			let vals: Vec<u64> = values
				.iter()
				.map(|v| match v {
					Value::Uint8(n) => *n,
					_ => 0,
				})
				.collect();
			ColumnData::uint8(vals)
		}
		_ => {
			// For other types, convert to string representation
			let vals: Vec<String> = values.iter().map(|v| format!("{:?}", v)).collect();
			ColumnData::utf8(vals)
		}
	};

	Ok(Column {
		name: Fragment::internal("value"),
		data,
	})
}
