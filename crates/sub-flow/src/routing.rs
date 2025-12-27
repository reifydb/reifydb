// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! CDC event routing to flow tasks.

use std::collections::HashMap;

use reifydb_core::{
	CowVec, Result, Row,
	interface::{Cdc, CdcChange, Engine, PrimitiveId, catalog::FlowId},
	key::Key,
	value::encoded::{EncodedValues, EncodedValuesNamedLayout},
};
use reifydb_engine::StandardEngine;
use reifydb_flow_operator_sdk::{FlowChange, FlowDiff};
use tracing::{trace, warn};

use crate::{catalog::FlowCatalog, registry::FlowRegistry};

/// Route CDC data events to interested flows.
///
/// Groups events by flow and sends batches through their channels.
pub async fn route_to_flows(registry: &FlowRegistry, engine: &StandardEngine, cdc: &Cdc) -> Result<()> {
	let version = cdc.version;

	// Get read locks
	let source_map = registry.source_to_flows.read().await;
	let flows = registry.flows.read().await;

	// Group events by flow
	let mut flow_batches: HashMap<FlowId, Vec<FlowChange>> = HashMap::new();

	// Use catalog cache for row decoding
	let catalog_cache = FlowCatalog::new();

	// Create a single transaction for all row decoding (much faster than per-change txns)
	let mut txn = engine.begin_query().await?;

	for cdc_change in &cdc.changes {
		// Only process row changes (data events)
		let Some(decoded_key) = Key::decode(cdc_change.key()) else {
			continue;
		};

		let Key::Row(row_key) = decoded_key else {
			continue;
		};

		let source_id = row_key.primitive;
		let row_number = row_key.row;

		// Find flows that subscribe to this source
		let Some(flow_ids) = source_map.get(&source_id) else {
			continue;
		};

		// Convert CDC change to FlowChange
		let flow_change = match convert_cdc_to_flow_change(
			&mut txn,
			&catalog_cache,
			source_id,
			row_number,
			&cdc_change.change,
			version,
		)
		.await
		{
			Ok(change) => change,
			Err(e) => {
				warn!(source = ?source_id, row = row_number.0, error = %e, "failed to decode row");
				continue;
			}
		};

		// Add to each interested flow's batch
		for &flow_id in flow_ids {
			flow_batches.entry(flow_id).or_default().push(flow_change.clone());
		}
	}

	// Drop transaction before sending to channels
	drop(txn);

	// Send batches to flow channels
	for (flow_id, changes) in flow_batches {
		if changes.is_empty() {
			continue;
		}

		let Some(handle) = flows.get(&flow_id) else {
			// Flow was deleted between grouping and sending
			trace!(flow_id = flow_id.0, "flow deleted, skipping batch");
			continue;
		};

		// Send to flow's channel (unbounded, never blocks)
		if handle.tx.send(changes).is_err() {
			// Flow task has exited - this shouldn't happen in normal operation
			panic!("flow {} channel closed unexpectedly", flow_id.0);
		}
	}

	drop(flows);
	drop(source_map);

	Ok(())
}

/// Convert CDC change format to FlowChange format.
pub(crate) async fn convert_cdc_to_flow_change(
	txn: &mut reifydb_engine::StandardQueryTransaction,
	catalog_cache: &FlowCatalog,
	source_id: PrimitiveId,
	row_number: reifydb_type::RowNumber,
	cdc_change: &CdcChange,
	version: reifydb_core::CommitVersion,
) -> Result<FlowChange> {
	match cdc_change {
		CdcChange::Insert {
			post,
			..
		} => {
			let row = create_row(txn, catalog_cache, source_id, row_number, post.to_vec()).await?;
			let diff = FlowDiff::Insert {
				post: row,
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
				pre: pre_row,
				post: post_row,
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
				pre: row,
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
	row_number: reifydb_type::RowNumber,
	row_bytes: Vec<u8>,
) -> Result<Row> {
	use reifydb_core::{
		Error,
		interface::{EncodableKey, QueryTransaction},
		key::DictionaryEntryIndexKey,
		value::encoded::EncodedValuesLayout,
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
