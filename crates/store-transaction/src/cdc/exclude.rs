// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::KeyKind;

/// Returns true if the KeyKind should be excluded from CDC generation.
///
/// Excluded kinds represent internal system state or operator bookkeeping
/// that should not generate user-facing change events:
/// - FlowNodeState: Operator state subject to retention policies
/// - FlowNodeInternalState: Operator internal state (e.g., join hash tables, row mappings)
/// - CdcConsumer: CDC consumption checkpoints
/// - StorageTracker: Internal storage statistics tracking
/// - SystemSequence, RowSequence, ColumnSequence, DictionarySequence: ID generators
/// - SystemVersion, TransactionVersion: Internal version tracking
/// - RingBufferMetadata: Ring buffer internal state (head/tail pointers)
/// - Index: Index metadata (derived from Row changes)
pub(crate) fn should_exclude_from_cdc(kind: KeyKind) -> bool {
	matches!(
		kind,
		// Flow operator state
		KeyKind::FlowNodeState
			| KeyKind::FlowNodeInternalState
		// CDC infrastructure
			| KeyKind::CdcConsumer
		// Internal tracking and statistics
			| KeyKind::StorageTracker
		// Sequence generators (internal ID generation)
			| KeyKind::SystemSequence
			| KeyKind::RowSequence
			| KeyKind::ColumnSequence
			| KeyKind::DictionarySequence
		// Version tracking (internal system state)
			| KeyKind::SystemVersion
			| KeyKind::TransactionVersion
			| KeyKind::FlowVersion
		// Ring buffer internal bookkeeping
			| KeyKind::RingBufferMetadata
		// Index metadata (derived from Row CDC)
			| KeyKind::Index
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_all_key_kinds_have_explicit_cdc_decision() {
		// This test ensures all KeyKind variants are explicitly considered for CDC exclusion.
		// When you add a new KeyKind variant:
		// 1. Add it to the match below (compiler will force you)
		// 2. Add a test: test_exclude_<name>() or test_include_<name>()
		// 3. If excluding, add to should_exclude_from_cdc()

		let test_variant = KeyKind::Row; // Use any variant to test the match

		match test_variant {
			KeyKind::Namespace => {}
			KeyKind::Table => {}
			KeyKind::Row => {}
			KeyKind::NamespaceTable => {}
			KeyKind::SystemSequence => {}
			KeyKind::Columns => {}
			KeyKind::Column => {}
			KeyKind::RowSequence => {}
			KeyKind::ColumnPolicy => {}
			KeyKind::SystemVersion => {}
			KeyKind::TransactionVersion => {}
			KeyKind::Index => {}
			KeyKind::IndexEntry => {}
			KeyKind::ColumnSequence => {}
			KeyKind::CdcConsumer => {}
			KeyKind::View => {}
			KeyKind::NamespaceView => {}
			KeyKind::PrimaryKey => {}
			KeyKind::FlowNodeState => {}
			KeyKind::RingBuffer => {}
			KeyKind::NamespaceRingBuffer => {}
			KeyKind::RingBufferMetadata => {}
			KeyKind::PrimitiveRetentionPolicy => {}
			KeyKind::OperatorRetentionPolicy => {}
			KeyKind::Flow => {}
			KeyKind::NamespaceFlow => {}
			KeyKind::FlowNode => {}
			KeyKind::FlowNodeByFlow => {}
			KeyKind::FlowEdge => {}
			KeyKind::FlowEdgeByFlow => {}
			KeyKind::FlowNodeInternalState => {}
			KeyKind::Dictionary => {}
			KeyKind::DictionaryEntry => {}
			KeyKind::DictionaryEntryIndex => {}
			KeyKind::NamespaceDictionary => {}
			KeyKind::DictionarySequence => {}
			KeyKind::StorageTracker => {}
			KeyKind::FlowVersion => {} /* When adding a new variant, add it here.
			                            * The compiler will error if you forget.
			                            * Then add a test and update should_exclude_from_cdc() if
			                            * needed. */
		}
	}

	// Tests for excluded KeyKinds (should return true)

	// Flow operator state
	#[test]
	fn test_exclude_flow_node_state() {
		assert!(should_exclude_from_cdc(KeyKind::FlowNodeState));
	}

	#[test]
	fn test_exclude_flow_node_internal_state() {
		assert!(should_exclude_from_cdc(KeyKind::FlowNodeInternalState));
	}

	// CDC infrastructure
	#[test]
	fn test_exclude_cdc_consumer() {
		assert!(should_exclude_from_cdc(KeyKind::CdcConsumer));
	}

	// Internal tracking and statistics
	#[test]
	fn test_exclude_storage_tracker() {
		assert!(should_exclude_from_cdc(KeyKind::StorageTracker));
	}

	// Sequence generators
	#[test]
	fn test_exclude_system_sequence() {
		assert!(should_exclude_from_cdc(KeyKind::SystemSequence));
	}

	#[test]
	fn test_exclude_row_sequence() {
		assert!(should_exclude_from_cdc(KeyKind::RowSequence));
	}

	#[test]
	fn test_exclude_column_sequence() {
		assert!(should_exclude_from_cdc(KeyKind::ColumnSequence));
	}

	#[test]
	fn test_exclude_dictionary_sequence() {
		assert!(should_exclude_from_cdc(KeyKind::DictionarySequence));
	}

	// Version tracking
	#[test]
	fn test_exclude_system_version() {
		assert!(should_exclude_from_cdc(KeyKind::SystemVersion));
	}

	#[test]
	fn test_exclude_transaction_version() {
		assert!(should_exclude_from_cdc(KeyKind::TransactionVersion));
	}

	// Ring buffer internal bookkeeping
	#[test]
	fn test_exclude_ring_buffer_metadata() {
		assert!(should_exclude_from_cdc(KeyKind::RingBufferMetadata));
	}

	// Index metadata
	#[test]
	fn test_exclude_index() {
		assert!(should_exclude_from_cdc(KeyKind::Index));
	}

	// Tests for KeyKinds that should generate CDC (should return false)

	#[test]
	fn test_include_namespace() {
		assert!(!should_exclude_from_cdc(KeyKind::Namespace));
	}

	#[test]
	fn test_include_table() {
		assert!(!should_exclude_from_cdc(KeyKind::Table));
	}

	#[test]
	fn test_include_row() {
		assert!(!should_exclude_from_cdc(KeyKind::Row));
	}

	#[test]
	fn test_include_namespace_table() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceTable));
	}

	#[test]
	fn test_include_columns() {
		assert!(!should_exclude_from_cdc(KeyKind::Columns));
	}

	#[test]
	fn test_include_column() {
		assert!(!should_exclude_from_cdc(KeyKind::Column));
	}

	#[test]
	fn test_include_column_policy() {
		assert!(!should_exclude_from_cdc(KeyKind::ColumnPolicy));
	}

	#[test]
	fn test_include_index_entry() {
		assert!(!should_exclude_from_cdc(KeyKind::IndexEntry));
	}

	#[test]
	fn test_include_view() {
		assert!(!should_exclude_from_cdc(KeyKind::View));
	}

	#[test]
	fn test_include_namespace_view() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceView));
	}

	#[test]
	fn test_include_primary_key() {
		assert!(!should_exclude_from_cdc(KeyKind::PrimaryKey));
	}

	#[test]
	fn test_include_ring_buffer() {
		assert!(!should_exclude_from_cdc(KeyKind::RingBuffer));
	}

	#[test]
	fn test_include_namespace_ring_buffer() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceRingBuffer));
	}

	#[test]
	fn test_include_source_retention_policy() {
		assert!(!should_exclude_from_cdc(KeyKind::PrimitiveRetentionPolicy));
	}

	#[test]
	fn test_include_operator_retention_policy() {
		assert!(!should_exclude_from_cdc(KeyKind::OperatorRetentionPolicy));
	}

	#[test]
	fn test_include_flow() {
		assert!(!should_exclude_from_cdc(KeyKind::Flow));
	}

	#[test]
	fn test_include_namespace_flow() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceFlow));
	}

	#[test]
	fn test_include_flow_node() {
		assert!(!should_exclude_from_cdc(KeyKind::FlowNode));
	}

	#[test]
	fn test_include_flow_node_by_flow() {
		assert!(!should_exclude_from_cdc(KeyKind::FlowNodeByFlow));
	}

	#[test]
	fn test_include_flow_edge() {
		assert!(!should_exclude_from_cdc(KeyKind::FlowEdge));
	}

	#[test]
	fn test_include_flow_edge_by_flow() {
		assert!(!should_exclude_from_cdc(KeyKind::FlowEdgeByFlow));
	}

	#[test]
	fn test_include_dictionary() {
		assert!(!should_exclude_from_cdc(KeyKind::Dictionary));
	}

	#[test]
	fn test_include_dictionary_entry() {
		assert!(!should_exclude_from_cdc(KeyKind::DictionaryEntry));
	}

	#[test]
	fn test_include_dictionary_entry_index() {
		assert!(!should_exclude_from_cdc(KeyKind::DictionaryEntryIndex));
	}

	#[test]
	fn test_include_namespace_dictionary() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceDictionary));
	}
}
