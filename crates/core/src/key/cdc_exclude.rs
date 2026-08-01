// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::KeyKind;

pub fn should_exclude_from_cdc(kind: KeyKind) -> bool {
	matches!(
		kind,
		KeyKind::OperatorState
			| KeyKind::CdcConsumer
			| KeyKind::Metric | KeyKind::SystemSequence
			| KeyKind::RowSequence
			| KeyKind::ColumnSequence
			| KeyKind::SystemVersion
			| KeyKind::TransactionVersion
			| KeyKind::FlowVersion
			| KeyKind::RingBufferMetadata
			| KeyKind::Index | KeyKind::Subscription
			| KeyKind::SubscriptionColumn
			| KeyKind::SubscriptionRow
			| KeyKind::ConfigStorage
			| KeyKind::Token | KeyKind::VersionEpoch
	)
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_all_key_kinds_have_explicit_cdc_decision() {
		// The exhaustive match forces every new KeyKind to make a CDC-exclusion decision instead of
		// silently defaulting into the log.

		let test_variant = KeyKind::Row;

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
			KeyKind::OperatorState => {}
			KeyKind::RingBuffer => {}
			KeyKind::NamespaceRingBuffer => {}
			KeyKind::RingBufferMetadata => {}
			KeyKind::Flow => {}
			KeyKind::NamespaceFlow => {}
			KeyKind::Operator => {}
			KeyKind::OperatorByFlow => {}
			KeyKind::FlowEdge => {}
			KeyKind::FlowEdgeByFlow => {}
			KeyKind::Dictionary => {}
			KeyKind::DictionaryEntry => {}
			KeyKind::DictionaryEntryIndex => {}
			KeyKind::NamespaceDictionary => {}
			KeyKind::Metric => {}
			KeyKind::FlowVersion => {}
			KeyKind::Subscription => {}
			KeyKind::SubscriptionRow => {}
			KeyKind::SubscriptionColumn => {}
			KeyKind::RowShape => {}
			KeyKind::SumType => {}
			KeyKind::NamespaceSumType => {}
			KeyKind::RowShapeField => {}
			KeyKind::Handler => {}
			KeyKind::NamespaceHandler => {}
			KeyKind::VariantHandler => {}
			KeyKind::Series => {}
			KeyKind::NamespaceSeries => {}
			KeyKind::SeriesMetadata => {}
			KeyKind::Identity => {}
			KeyKind::Role => {}
			KeyKind::GrantedRole => {}
			KeyKind::Policy => {}
			KeyKind::PolicyOp => {}
			KeyKind::Migration => {}
			KeyKind::Authentication => {}
			KeyKind::MigrationEvent => {}
			KeyKind::ConfigStorage => {}
			KeyKind::Token => {}
			KeyKind::Source => {}
			KeyKind::NamespaceSource => {}
			KeyKind::Sink => {}
			KeyKind::NamespaceSink => {}
			KeyKind::SourceCheckpoint => {}
			KeyKind::RowSettings => {}
			KeyKind::Procedure => {}
			KeyKind::NamespaceProcedure => {}
			KeyKind::ProcedureParam => {}
			KeyKind::Binding => {}
			KeyKind::OperatorSettings => {}
			KeyKind::NamespaceBinding => {}
			KeyKind::ColumnSnapshot => {}
			KeyKind::SeriesColumnSnapshot => {}
			KeyKind::TableColumnSnapshot => {}
			KeyKind::IdentityAttribute => {}
			KeyKind::IdentityAttributeValue => {}
			KeyKind::PartitionedRow => {}
			KeyKind::Partition => {}
			KeyKind::Queue => {}
			KeyKind::NamespaceQueue => {}
			KeyKind::QueueDeduplication => {}
			KeyKind::VersionEpoch => {}
		}
	}

	#[test]
	fn test_exclude_operator_state() {
		assert!(should_exclude_from_cdc(KeyKind::OperatorState));
	}

	#[test]
	fn test_exclude_cdc_consumer() {
		assert!(should_exclude_from_cdc(KeyKind::CdcConsumer));
	}

	#[test]
	fn test_exclude_storage_tracker() {
		assert!(should_exclude_from_cdc(KeyKind::Metric));
	}

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
	fn test_exclude_system_version() {
		assert!(should_exclude_from_cdc(KeyKind::SystemVersion));
	}

	#[test]
	fn test_exclude_transaction_version() {
		assert!(should_exclude_from_cdc(KeyKind::TransactionVersion));
	}

	#[test]
	fn test_exclude_ring_buffer_metadata() {
		assert!(should_exclude_from_cdc(KeyKind::RingBufferMetadata));
	}

	#[test]
	fn test_exclude_index() {
		assert!(should_exclude_from_cdc(KeyKind::Index));
	}

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
	fn test_include_partitioned_row() {
		assert!(!should_exclude_from_cdc(KeyKind::PartitionedRow));
	}

	#[test]
	fn test_include_partition() {
		assert!(!should_exclude_from_cdc(KeyKind::Partition));
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
	fn test_include_queue() {
		assert!(!should_exclude_from_cdc(KeyKind::Queue));
	}

	#[test]
	fn test_include_namespace_queue() {
		assert!(!should_exclude_from_cdc(KeyKind::NamespaceQueue));
	}

	#[test]
	fn test_include_queue_deduplication() {
		assert!(!should_exclude_from_cdc(KeyKind::QueueDeduplication));
	}

	#[test]
	fn test_include_operator_settings() {
		assert!(!should_exclude_from_cdc(KeyKind::OperatorSettings));
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
	fn test_include_operator() {
		assert!(!should_exclude_from_cdc(KeyKind::Operator));
	}

	#[test]
	fn test_include_operator_by_flow() {
		assert!(!should_exclude_from_cdc(KeyKind::OperatorByFlow));
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
	fn test_include_shape() {
		assert!(!should_exclude_from_cdc(KeyKind::RowShape));
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
	fn test_include_shape_field() {
		assert!(!should_exclude_from_cdc(KeyKind::RowShapeField));
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
	fn test_include_identity() {
		assert!(!should_exclude_from_cdc(KeyKind::Identity));
	}

	#[test]
	fn test_include_role() {
		assert!(!should_exclude_from_cdc(KeyKind::Role));
	}

	#[test]
	fn test_include_granted_role() {
		assert!(!should_exclude_from_cdc(KeyKind::GrantedRole));
	}

	#[test]
	fn test_include_identity_attribute() {
		assert!(!should_exclude_from_cdc(KeyKind::IdentityAttribute));
	}

	#[test]
	fn test_include_identity_attribute_value() {
		assert!(!should_exclude_from_cdc(KeyKind::IdentityAttributeValue));
	}

	#[test]
	fn test_include_authentication() {
		assert!(!should_exclude_from_cdc(KeyKind::Authentication));
	}

	#[test]
	fn test_include_policy() {
		assert!(!should_exclude_from_cdc(KeyKind::Policy));
	}

	#[test]
	fn test_include_policy_op() {
		assert!(!should_exclude_from_cdc(KeyKind::PolicyOp));
	}

	#[test]
	fn test_include_migration() {
		assert!(!should_exclude_from_cdc(KeyKind::Migration));
	}

	#[test]
	fn test_include_migration_event() {
		assert!(!should_exclude_from_cdc(KeyKind::MigrationEvent));
	}

	#[test]
	fn test_exclude_flow_version() {
		assert!(should_exclude_from_cdc(KeyKind::FlowVersion));
	}

	#[test]
	fn test_exclude_config() {
		assert!(should_exclude_from_cdc(KeyKind::ConfigStorage));
	}

	#[test]
	fn test_exclude_version_epoch() {
		assert!(should_exclude_from_cdc(KeyKind::VersionEpoch));
	}
}
