// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC event conversion utilities for flows.

use reifydb_core::{
	common::CommitVersion,
	encoded::{encoded::EncodedValues, schema::Schema},
	interface::{
		catalog::primitive::PrimitiveId,
		cdc::{Cdc, CdcChange},
	},
	internal,
	key::{EncodableKey, Key, dictionary::DictionaryEntryIndexKey},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_sdk::flow::{FlowChange, FlowDiff};
use reifydb_transaction::standard::query::StandardQueryTransaction;
use reifydb_type::{
	Result,
	error::Error,
	util::cowvec::CowVec,
	value::{Value, dictionary::DictionaryEntryId, row_number::RowNumber},
};
use tracing::{Span, instrument, warn};

use crate::catalog::FlowCatalog;

/// Convert CDC change format to FlowChange format.
pub(crate) fn convert_cdc_to_flow_change(
	txn: &mut StandardQueryTransaction,
	catalog_cache: &FlowCatalog,
	source_id: PrimitiveId,
	row_number: RowNumber,
	cdc_change: &CdcChange,
	version: CommitVersion,
) -> Result<FlowChange> {
	match cdc_change {
		CdcChange::Insert {
			post,
			..
		} => {
			let row = create_row(txn, catalog_cache, source_id, row_number, post.to_vec())?;
			let diff = FlowDiff::Insert {
				post: Columns::from_row(&row),
			};
			Ok(FlowChange::external(source_id, version, vec![diff]))
		}
		CdcChange::Update {
			pre,
			post,
			..
		} => {
			let pre_row = create_row(txn, catalog_cache, source_id, row_number, pre.to_vec())?;
			let post_row = create_row(txn, catalog_cache, source_id, row_number, post.to_vec())?;
			let diff = FlowDiff::Update {
				pre: Columns::from_row(&pre_row),
				post: Columns::from_row(&post_row),
			};
			Ok(FlowChange::external(source_id, version, vec![diff]))
		}
		CdcChange::Delete {
			pre,
			..
		} => {
			let diff = if let Some(pre_values) = pre {
				let pre_bytes = pre_values.to_vec();
				let row = create_row(txn, catalog_cache, source_id, row_number, pre_bytes)?;
				FlowDiff::Remove {
					pre: Columns::from_row(&row),
				}
			} else {
				// No pre-image available, create remove with empty columns
				FlowDiff::Remove {
					pre: Columns::new(Vec::new()),
				}
			};
			Ok(FlowChange::external(source_id, version, vec![diff]))
		}
	}
}

/// Create a Row from encoded bytes, handling dictionary decoding.
pub(crate) fn create_row(
	txn: &mut StandardQueryTransaction,
	catalog_cache: &FlowCatalog,
	source_id: PrimitiveId,
	row_number: RowNumber,
	row_bytes: Vec<u8>,
) -> Result<Row> {
	// Get cached source metadata (loads from catalog on cache miss)
	let metadata = catalog_cache.get_or_load(txn, source_id)?;

	// Handle empty row bytes - return error if table has columns
	if row_bytes.is_empty() && !metadata.value_types.is_empty() {
		return Err(Error(internal!(
			"Cannot decode empty row bytes for table with {} columns",
			metadata.value_types.len()
		)));
	}

	let raw_encoded = EncodedValues(CowVec::new(row_bytes));

	// If no dictionary columns, return directly with value schema
	if !metadata.has_dictionary_columns {
		let fields: Vec<reifydb_core::encoded::schema::SchemaField> = metadata
			.value_types
			.iter()
			.map(|(name, ty)| reifydb_core::encoded::schema::SchemaField::unconstrained(name.clone(), *ty))
			.collect();
		let schema = Schema::new(fields);
		return Ok(Row {
			number: row_number,
			encoded: raw_encoded,
			schema,
		});
	}

	// Decode dictionary columns
	let storage_schema = Schema::testing(&metadata.storage_types);
	let fields: Vec<reifydb_core::encoded::schema::SchemaField> = metadata
		.value_types
		.iter()
		.map(|(name, ty)| reifydb_core::encoded::schema::SchemaField::unconstrained(name.clone(), *ty))
		.collect();
	let value_schema = Schema::new(fields);

	let mut values: Vec<Value> = Vec::with_capacity(metadata.dictionaries.len());
	for (idx, dict_opt) in metadata.dictionaries.iter().enumerate() {
		let raw_value = storage_schema.get_value(&raw_encoded, idx);

		if let Some(dictionary) = dict_opt {
			// Decode dictionary ID to actual value
			if let Some(entry_id) = DictionaryEntryId::from_value(&raw_value) {
				let index_key =
					DictionaryEntryIndexKey::new(dictionary.id, entry_id.to_u128() as u64).encode();
				match txn.get(&index_key)? {
					Some(v) => {
						let decoded_value: Value =
							postcard::from_bytes(&v.values).map_err(|e| {
								Error(internal!(
									"Failed to deserialize dictionary value: {}",
									e
								))
							})?;
						values.push(decoded_value);
					}
					None => {
						values.push(Value::Undefined);
					}
				}
			} else {
				values.push(raw_value);
			}
		} else {
			values.push(raw_value);
		}
	}

	// Re-encode with value schema
	let mut encoded = value_schema.allocate();
	value_schema.set_values(&mut encoded, &values);

	Ok(Row {
		number: row_number,
		encoded,
		schema: value_schema,
	})
}

/// Convert all CDC changes in a batch to FlowChanges.
///
/// Processes all row changes from the CDC batch, skipping non-row changes
/// and delete events without pre-images.
#[instrument(name = "flow::convert::to_flow_change", level = "debug", skip(engine, catalog_cache, cdc), fields(
	input = cdc.changes.len(),
	output = tracing::field::Empty,
	elapsed_us = tracing::field::Empty
))]
pub(crate) fn to_flow_change(
	engine: &StandardEngine,
	catalog_cache: &FlowCatalog,
	cdc: &Cdc,
	version: CommitVersion,
) -> Result<Vec<FlowChange>> {
	let start = reifydb_runtime::time::Instant::now();
	let mut changes = Vec::new();

	let mut query_txn = engine.begin_query_at_version(version)?;

	for cdc_change in &cdc.changes {
		if let Some(Key::Row(row_key)) = Key::decode(cdc_change.key()) {
			let source_id = row_key.primitive;
			let row_number = row_key.row;

			match convert_cdc_to_flow_change(
				&mut query_txn,
				catalog_cache,
				source_id,
				row_number,
				&cdc_change.change,
				version,
			) {
				Ok(change) => changes.push(change),
				Err(e) => {
					warn!(
						source = ?source_id,
						row = row_number.0,
						error = %e,
						"failed to decode CDC change"
					);
				}
			}
		}
	}

	Span::current().record("output", changes.len());
	Span::current().record("elapsed_us", start.elapsed().as_micros() as u64);
	Ok(changes)
}
