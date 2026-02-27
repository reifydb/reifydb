// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Key exclusion logic for CDC generation.
//!
//! Determines which key kinds should be excluded from CDC to avoid
//! generating events for internal system state.

use super::KeyKind;

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
pub fn should_exclude_from_cdc(kind: KeyKind) -> bool {
	matches!(
		kind,
		// Flow operator state
		KeyKind::FlowNodeState
			| KeyKind::FlowNodeInternalState
		// CDC infrastructure
			| KeyKind::CdcConsumer
		// Internal tracking and statistics
			| KeyKind::Metric
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
		// Subscriptions are runtime only
			| KeyKind::Subscription
			| KeyKind::SubscriptionColumn
			| KeyKind::SubscriptionRow
	)
}

#[cfg(test)]
pub mod tests {
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
			KeyKind::ColumnProperty => {}
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
			KeyKind::Metric => {}
			KeyKind::FlowVersion => {}
			KeyKind::Subscription => {}
			KeyKind::SubscriptionRow => {}
			KeyKind::SubscriptionColumn => {}
			KeyKind::Schema => {}
			KeyKind::SumType => {}
			KeyKind::NamespaceSumType => {}
			KeyKind::SchemaField => {}
			KeyKind::Handler => {}
			KeyKind::NamespaceHandler => {}
			KeyKind::VariantHandler => {}
			KeyKind::Series => {}
			KeyKind::NamespaceSeries => {}
			KeyKind::SeriesMetadata => {}
			KeyKind::User => {}
			KeyKind::Role => {}
			KeyKind::UserRole => {}
			KeyKind::SecurityPolicy => {}
			KeyKind::SecurityPolicyOp => {}
			KeyKind::Migration => {}
			KeyKind::UserAuthentication => {}
			KeyKind::MigrationEvent => {} /* When adding a new variant, add it here.
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
		assert!(should_exclude_from_cdc(KeyKind::Metric));
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

	// Subscriptions (runtime only)
	#[test]
	fn test_exclude_subscription() {
		assert!(should_exclude_from_cdc(KeyKind::Subscription));
	}

	#[test]
	fn test_exclude_subscription_column() {
		assert!(should_exclude_from_cdc(KeyKind::SubscriptionColumn));
	}

	#[test]
	fn test_exclude_subscription_row() {
		assert!(should_exclude_from_cdc(KeyKind::SubscriptionRow));
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
	fn test_include_column_property() {
		assert!(!should_exclude_from_cdc(KeyKind::ColumnProperty));
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

	#[test]
	fn test_include_handler() {
		assert!(!should_exclude_from_cdc(KeyKind::Handler));
	}

	#[test]
	fn test_include_namespace_handler() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceHandler));
	}

	#[test]
	fn test_include_variant_handler() {
		assert!(!should_exclude_from_cdc(KeyKind::VariantHandler));
	}

	#[test]
	fn test_include_schema() {
		assert!(!should_exclude_from_cdc(KeyKind::Schema));
	}

	#[test]
	fn test_include_sum_type() {
		assert!(!should_exclude_from_cdc(KeyKind::SumType));
	}

	#[test]
	fn test_include_namespace_sum_type() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceSumType));
	}

	#[test]
	fn test_include_schema_field() {
		assert!(!should_exclude_from_cdc(KeyKind::SchemaField));
	}

	#[test]
	fn test_include_series() {
		assert!(!should_exclude_from_cdc(KeyKind::Series));
	}

	#[test]
	fn test_include_namespace_series() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceSeries));
	}

	#[test]
	fn test_include_series_metadata() {
		assert!(!should_exclude_from_cdc(KeyKind::SeriesMetadata));
	}

	#[test]
	fn test_include_user() {
		assert!(!should_exclude_from_cdc(KeyKind::User));
	}

	#[test]
	fn test_include_role() {
		assert!(!should_exclude_from_cdc(KeyKind::Role));
	}

	#[test]
	fn test_include_user_role() {
		assert!(!should_exclude_from_cdc(KeyKind::UserRole));
	}

	#[test]
	fn test_include_user_authentication() {
		assert!(!should_exclude_from_cdc(KeyKind::UserAuthentication));
	}

	#[test]
	fn test_include_security_policy() {
		assert!(!should_exclude_from_cdc(KeyKind::SecurityPolicy));
	}

	#[test]
	fn test_include_security_policy_op() {
		assert!(!should_exclude_from_cdc(KeyKind::SecurityPolicyOp));
	}

	#[test]
	fn test_include_migration() {
		assert!(!should_exclude_from_cdc(KeyKind::Migration));
	}

	#[test]
	fn test_include_migration_event() {
		assert!(!should_exclude_from_cdc(KeyKind::MigrationEvent));
	}

	// Version tracking (excluded)
	#[test]
	fn test_exclude_flow_version() {
		assert!(should_exclude_from_cdc(KeyKind::FlowVersion));
	}
}
