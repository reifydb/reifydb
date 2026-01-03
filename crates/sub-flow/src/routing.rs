// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC event conversion utilities for flows.

use reifydb_core::{
	CommitVersion, CowVec, Result, Row,
	interface::{CdcChange, PrimitiveId},
	value::{
		column::Columns,
		encoded::{EncodedValues, EncodedValuesNamedLayout},
	},
};
use reifydb_sdk::{FlowChange, FlowDiff};
use reifydb_type::RowNumber;

use crate::catalog::FlowCatalog;

/// Convert CDC change format to FlowChange format.
pub(crate) async fn convert_cdc_to_flow_change(
	txn: &mut reifydb_engine::StandardQueryTransaction,
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
			let row = create_row(txn, catalog_cache, source_id, row_number, post.to_vec()).await?;
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
			let pre_row = create_row(txn, catalog_cache, source_id, row_number, pre.to_vec()).await?;
			let post_row = create_row(txn, catalog_cache, source_id, row_number, post.to_vec()).await?;
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
			let pre_bytes = pre.as_ref().map(|v| v.to_vec()).unwrap_or_default();
			let row = create_row(txn, catalog_cache, source_id, row_number, pre_bytes).await?;
			let diff = FlowDiff::Remove {
				pre: Columns::from_row(&row),
			};
			Ok(FlowChange::external(source_id, version, vec![diff]))
		}
	}
}

/// Create a Row from encoded bytes, handling dictionary decoding.
pub(crate) async fn create_row(
	txn: &mut reifydb_engine::StandardQueryTransaction,
	catalog_cache: &FlowCatalog,
	source_id: PrimitiveId,
	row_number: RowNumber,
	row_bytes: Vec<u8>,
) -> Result<Row> {
	use reifydb_core::{
		Error, interface::EncodableKey, key::DictionaryEntryIndexKey, value::encoded::EncodedValuesLayout,
	};
	use reifydb_type::{DictionaryEntryId, Value, internal};

	// Get cached source metadata (loads from catalog on cache miss)
	let metadata = catalog_cache.get_or_load(txn, source_id).await?;

	let raw_encoded = EncodedValues(CowVec::new(row_bytes));

	// If no dictionary columns, return directly with value layout
	if !metadata.has_dictionary_columns {
		let layout = EncodedValuesNamedLayout::new(metadata.value_types.clone());
		return Ok(Row {
			number: row_number,
			encoded: raw_encoded,
			layout,
		});
	}

	// Decode dictionary columns
	let storage_layout = EncodedValuesLayout::new(&metadata.storage_types);
	let value_layout = EncodedValuesNamedLayout::new(metadata.value_types.clone());

	let mut values: Vec<Value> = Vec::with_capacity(metadata.dictionaries.len());
	for (idx, dict_opt) in metadata.dictionaries.iter().enumerate() {
		let raw_value = storage_layout.get_value(&raw_encoded, idx);

		if let Some(dictionary) = dict_opt {
			// Decode dictionary ID to actual value
			if let Some(entry_id) = DictionaryEntryId::from_value(&raw_value) {
				let index_key =
					DictionaryEntryIndexKey::new(dictionary.id, entry_id.to_u128() as u64).encode();
				match txn.get(&index_key).await? {
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

	// Re-encode with value layout
	let mut encoded = value_layout.allocate();
	value_layout.set_values(&mut encoded, &values);

	Ok(Row {
		number: row_number,
		encoded,
		layout: value_layout,
	})
}
