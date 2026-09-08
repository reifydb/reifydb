// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use reifydb_codec::key::{deserializer::KeyDeserializer, encoded::EncodedKey, serializer::KeySerializer};
use smallvec::{SmallVec, smallvec};

use super::KeyTag;
use crate::{
	interface::{catalog::flow::FlowId, cdc::CdcConsumerId},
	key::{
		any::{ByteEncoding, Field, KeyFields},
		bound::TaggedKeyBoundRange,
	},
};

pub trait ToConsumerKey {
	fn to_consumer_key(&self) -> CdcConsumerKey;
}

impl ToConsumerKey for CdcConsumerKey {
	fn to_consumer_key(&self) -> CdcConsumerKey {
		self.clone()
	}
}

impl ToConsumerKey for CdcConsumerId {
	fn to_consumer_key(&self) -> CdcConsumerKey {
		CdcConsumerKey {
			consumer: self.clone(),
		}
	}
}

impl ToConsumerKey for FlowId {
	fn to_consumer_key(&self) -> CdcConsumerKey {
		CdcConsumerKey::new(CdcConsumerId::new(format!("flow:{}", self.0)))
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CdcConsumerKey {
	pub consumer: CdcConsumerId,
}

impl CdcConsumerKey {
	pub fn new(consumer: impl Into<CdcConsumerId>) -> Self {
		Self {
			consumer: consumer.into(),
		}
	}

	pub fn encoded(consumer: impl Into<CdcConsumerId>) -> EncodedKey {
		Self {
			consumer: consumer.into(),
		}
		.encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

impl CdcConsumerKey {
	pub const TAG: KeyTag = KeyTag::CdcConsumer;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(Self::TAG as u8).extend_str(&self.consumer);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self>
	where
		Self: Sized,
	{
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::TAG {
			return None;
		}

		let consumer_id = de.read_str().ok()?;

		Some(Self {
			consumer: CdcConsumerId(consumer_id),
		})
	}
}

#[cfg(test)]
pub mod cdc_consumer_key_tests {
	use std::ops::RangeBounds;

	use super::{CdcConsumerKey, ToConsumerKey};
	use crate::interface::{catalog::flow::FlowId, cdc::CdcConsumerId};

	#[test]
	fn test_encode_decode_cdc_consumer() {
		let key = CdcConsumerKey {
			consumer: CdcConsumerId::new("test-consumer"),
		};

		let encoded = key.encode();
		let decoded = CdcConsumerKey::decode(&encoded).expect("Failed to decode key");

		assert_eq!(decoded.consumer, CdcConsumerId::new("test-consumer"));
	}

	#[test]
	fn test_cdc_consumer_keys_within_range() {
		let key1 = CdcConsumerKey {
			consumer: CdcConsumerId::new("consumer-a"),
		}
		.encode();

		let key2 = CdcConsumerKey {
			consumer: CdcConsumerId::new("consumer-b"),
		}
		.encode();

		let key3 = CdcConsumerKey {
			consumer: CdcConsumerId::new("consumer-z"),
		}
		.encode();

		let range = CdcConsumerKey::full_scan().encode();

		assert!(range.contains(&key1), "consumer-a key should be in range");
		assert!(range.contains(&key2), "consumer-b key should be in range");
		assert!(range.contains(&key3), "consumer-z key should be in range");
	}

	#[test]
	fn test_flow_id_to_consumer_key() {
		let flow_id = FlowId(42);
		let encoded = flow_id.to_consumer_key().encode();

		let decoded = CdcConsumerKey::decode(&encoded).expect("Failed to decode key");
		assert_eq!(decoded.consumer, CdcConsumerId::new("flow:42"));
	}

	#[test]
	fn test_flow_id_keys_within_range() {
		let flow1 = FlowId(1).to_consumer_key().encode();
		let flow2 = FlowId(100).to_consumer_key().encode();
		let flow3 = FlowId(999).to_consumer_key().encode();

		let range = CdcConsumerKey::full_scan().encode();

		assert!(range.contains(&flow1), "flow:1 key should be in range");
		assert!(range.contains(&flow2), "flow:100 key should be in range");
		assert!(range.contains(&flow3), "flow:999 key should be in range");
	}
}

pub fn should_exclude_from_cdc(kind: KeyTag) -> bool {
	matches!(
		kind,
		KeyTag::OperatorState
			| KeyTag::CdcConsumer | KeyTag::Metric
			| KeyTag::SystemSequence
			| KeyTag::RowSequence | KeyTag::ColumnSequence
			| KeyTag::SystemVersion
			| KeyTag::TransactionVersion
			| KeyTag::FlowVersion | KeyTag::RingBufferMetadata
			| KeyTag::Index | KeyTag::ConfigStorage
			| KeyTag::Token | KeyTag::VersionEpoch
			| KeyTag::QueuePartition
			| KeyTag::QueueItemState
			| KeyTag::QueueDue | KeyTag::QueueKeyActive
	)
}

#[cfg(test)]
pub mod primary_key_tests {
	use super::*;

	#[test]
	fn test_all_key_kinds_have_explicit_cdc_decision() {
		// The exhaustive match forces every new KeyTag to make a CDC-exclusion decision instead of
		// silently defaulting into the log.

		let test_variant = KeyTag::Row;

		match test_variant {
			KeyTag::Namespace => {}
			KeyTag::Table => {}
			KeyTag::Row => {}
			KeyTag::NamespaceTable => {}
			KeyTag::SystemSequence => {}
			KeyTag::Columns => {}
			KeyTag::Column => {}
			KeyTag::RowSequence => {}
			KeyTag::ColumnProperty => {}
			KeyTag::SystemVersion => {}
			KeyTag::TransactionVersion => {}
			KeyTag::Index => {}
			KeyTag::IndexEntry => {}
			KeyTag::ColumnSequence => {}
			KeyTag::CdcConsumer => {}
			KeyTag::View => {}
			KeyTag::NamespaceView => {}
			KeyTag::PrimaryKey => {}
			KeyTag::OperatorState => {}
			KeyTag::RingBuffer => {}
			KeyTag::NamespaceRingBuffer => {}
			KeyTag::RingBufferMetadata => {}
			KeyTag::Flow => {}
			KeyTag::NamespaceFlow => {}
			KeyTag::Operator => {}
			KeyTag::OperatorByFlow => {}
			KeyTag::FlowEdge => {}
			KeyTag::FlowEdgeByFlow => {}
			KeyTag::OutputFrontier => {}
			KeyTag::Dictionary => {}
			KeyTag::DictionaryEntry => {}
			KeyTag::DictionaryEntryIndex => {}
			KeyTag::NamespaceDictionary => {}
			KeyTag::Metric => {}
			KeyTag::FlowVersion => {}
			KeyTag::RowShape => {}
			KeyTag::SumType => {}
			KeyTag::NamespaceSumType => {}
			KeyTag::RowShapeField => {}
			KeyTag::Handler => {}
			KeyTag::NamespaceHandler => {}
			KeyTag::VariantHandler => {}
			KeyTag::Series => {}
			KeyTag::NamespaceSeries => {}
			KeyTag::SeriesMetadata => {}
			KeyTag::Identity => {}
			KeyTag::Role => {}
			KeyTag::GrantedRole => {}
			KeyTag::Policy => {}
			KeyTag::PolicyOp => {}
			KeyTag::Migration => {}
			KeyTag::Authentication => {}
			KeyTag::MigrationEvent => {}
			KeyTag::ConfigStorage => {}
			KeyTag::Token => {}
			KeyTag::Source => {}
			KeyTag::NamespaceSource => {}
			KeyTag::Sink => {}
			KeyTag::NamespaceSink => {}
			KeyTag::RowSettings => {}
			KeyTag::Procedure => {}
			KeyTag::NamespaceProcedure => {}
			KeyTag::ProcedureParam => {}
			KeyTag::Binding => {}
			KeyTag::OperatorSettings => {}
			KeyTag::NamespaceBinding => {}
			KeyTag::ColumnSnapshot => {}
			KeyTag::SeriesColumnSnapshot => {}
			KeyTag::TableColumnSnapshot => {}
			KeyTag::IdentityAttribute => {}
			KeyTag::IdentityAttributeValue => {}
			KeyTag::PartitionedRow => {}
			KeyTag::PartitionedSeriesRow => {}
			KeyTag::Partition => {}
			KeyTag::Queue => {}
			KeyTag::NamespaceQueue => {}
			KeyTag::QueueDeduplication => {}
			KeyTag::QueuePartition => {}
			KeyTag::QueueItemState => {}
			KeyTag::QueueDue => {}
			KeyTag::QueueAttempt => {}
			KeyTag::QueueKeyActive => {}
			KeyTag::VersionEpoch => {}
			KeyTag::SeriesRow => {}
			KeyTag::SortedViewRow => {}
			KeyTag::PartitionedSortedViewRow => {}
			KeyTag::Relationship => {} /* When adding a new variant, add it here.
			                            * The compiler will error if you forget.
			                            * Then add a test and update should_exclude_from_cdc() if
			                            * needed. */
		}
	}

	#[test]
	fn test_sorted_view_rows_reach_the_cdc_log() {
		// A sorted view's rows carry their own kind, and a subscriber that never sees them reads a
		// view that silently stops changing.
		assert!(!should_exclude_from_cdc(KeyTag::SortedViewRow));
		assert!(!should_exclude_from_cdc(KeyTag::PartitionedSortedViewRow));
	}

	#[test]
	fn test_exclude_operator_state() {
		assert!(should_exclude_from_cdc(KeyTag::OperatorState));
	}

	#[test]
	fn test_exclude_cdc_consumer() {
		assert!(should_exclude_from_cdc(KeyTag::CdcConsumer));
	}

	#[test]
	fn test_exclude_storage_tracker() {
		assert!(should_exclude_from_cdc(KeyTag::Metric));
	}

	#[test]
	fn test_exclude_system_sequence() {
		assert!(should_exclude_from_cdc(KeyTag::SystemSequence));
	}

	#[test]
	fn test_exclude_row_sequence() {
		assert!(should_exclude_from_cdc(KeyTag::RowSequence));
	}

	#[test]
	fn test_exclude_column_sequence() {
		assert!(should_exclude_from_cdc(KeyTag::ColumnSequence));
	}

	#[test]
	fn test_exclude_system_version() {
		assert!(should_exclude_from_cdc(KeyTag::SystemVersion));
	}

	#[test]
	fn test_exclude_transaction_version() {
		assert!(should_exclude_from_cdc(KeyTag::TransactionVersion));
	}

	#[test]
	fn test_exclude_ring_buffer_metadata() {
		assert!(should_exclude_from_cdc(KeyTag::RingBufferMetadata));
	}

	#[test]
	fn test_exclude_index() {
		assert!(should_exclude_from_cdc(KeyTag::Index));
	}

	#[test]
	fn test_include_namespace() {
		assert!(!should_exclude_from_cdc(KeyTag::Namespace));
	}

	#[test]
	fn test_include_table() {
		assert!(!should_exclude_from_cdc(KeyTag::Table));
	}

	#[test]
	fn test_include_row() {
		assert!(!should_exclude_from_cdc(KeyTag::Row));
	}

	#[test]
	fn test_include_series_row() {
		// Series rows rode into the log under KeyTag::Row, so their own kind must keep them there.
		assert!(!should_exclude_from_cdc(KeyTag::SeriesRow));
	}

	#[test]
	fn test_include_partitioned_row() {
		assert!(!should_exclude_from_cdc(KeyTag::PartitionedRow));
	}

	#[test]
	fn test_include_partition() {
		assert!(!should_exclude_from_cdc(KeyTag::Partition));
	}

	#[test]
	fn test_include_namespace_table() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceTable));
	}

	#[test]
	fn test_include_columns() {
		assert!(!should_exclude_from_cdc(KeyTag::Columns));
	}

	#[test]
	fn test_include_column() {
		assert!(!should_exclude_from_cdc(KeyTag::Column));
	}

	#[test]
	fn test_include_column_property() {
		assert!(!should_exclude_from_cdc(KeyTag::ColumnProperty));
	}

	#[test]
	fn test_include_index_entry() {
		assert!(!should_exclude_from_cdc(KeyTag::IndexEntry));
	}

	#[test]
	fn test_include_view() {
		assert!(!should_exclude_from_cdc(KeyTag::View));
	}

	#[test]
	fn test_include_namespace_view() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceView));
	}

	#[test]
	fn test_include_primary_key() {
		assert!(!should_exclude_from_cdc(KeyTag::PrimaryKey));
	}

	#[test]
	fn test_include_ring_buffer() {
		assert!(!should_exclude_from_cdc(KeyTag::RingBuffer));
	}

	#[test]
	fn test_include_namespace_ring_buffer() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceRingBuffer));
	}

	#[test]
	fn test_include_queue() {
		assert!(!should_exclude_from_cdc(KeyTag::Queue));
	}

	#[test]
	fn test_include_namespace_queue() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceQueue));
	}

	#[test]
	fn test_include_queue_deduplication() {
		assert!(!should_exclude_from_cdc(KeyTag::QueueDeduplication));
	}

	#[test]
	fn test_exclude_queue_partition() {
		assert!(should_exclude_from_cdc(KeyTag::QueuePartition));
	}

	#[test]
	fn test_exclude_queue_item_state() {
		assert!(should_exclude_from_cdc(KeyTag::QueueItemState));
	}

	#[test]
	fn test_exclude_queue_due() {
		assert!(should_exclude_from_cdc(KeyTag::QueueDue));
	}

	#[test]
	fn test_exclude_queue_key_active() {
		assert!(should_exclude_from_cdc(KeyTag::QueueKeyActive));
	}

	#[test]
	fn test_include_queue_attempt() {
		// Attempt records are the durable audit trail of what a worker reported, not internal
		// scheduling churn. Excluding them would make every ack invisible to subscribers and
		// to any downstream view built on effect outcomes.
		assert!(!should_exclude_from_cdc(KeyTag::QueueAttempt));
	}

	#[test]
	fn test_include_operator_settings() {
		assert!(!should_exclude_from_cdc(KeyTag::OperatorSettings));
	}

	#[test]
	fn test_include_flow() {
		assert!(!should_exclude_from_cdc(KeyTag::Flow));
	}

	#[test]
	fn test_include_namespace_flow() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceFlow));
	}

	#[test]
	fn test_include_operator() {
		assert!(!should_exclude_from_cdc(KeyTag::Operator));
	}

	#[test]
	fn test_include_operator_by_flow() {
		assert!(!should_exclude_from_cdc(KeyTag::OperatorByFlow));
	}

	#[test]
	fn test_include_flow_edge() {
		assert!(!should_exclude_from_cdc(KeyTag::FlowEdge));
	}

	#[test]
	fn test_include_flow_edge_by_flow() {
		assert!(!should_exclude_from_cdc(KeyTag::FlowEdgeByFlow));
	}

	#[test]
	fn test_include_dictionary() {
		assert!(!should_exclude_from_cdc(KeyTag::Dictionary));
	}

	#[test]
	fn test_include_dictionary_entry() {
		assert!(!should_exclude_from_cdc(KeyTag::DictionaryEntry));
	}

	#[test]
	fn test_include_dictionary_entry_index() {
		assert!(!should_exclude_from_cdc(KeyTag::DictionaryEntryIndex));
	}

	#[test]
	fn test_include_namespace_dictionary() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceDictionary));
	}

	#[test]
	fn test_include_handler() {
		assert!(!should_exclude_from_cdc(KeyTag::Handler));
	}

	#[test]
	fn test_include_namespace_handler() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceHandler));
	}

	#[test]
	fn test_include_variant_handler() {
		assert!(!should_exclude_from_cdc(KeyTag::VariantHandler));
	}

	#[test]
	fn test_include_shape() {
		assert!(!should_exclude_from_cdc(KeyTag::RowShape));
	}

	#[test]
	fn test_include_sum_type() {
		assert!(!should_exclude_from_cdc(KeyTag::SumType));
	}

	#[test]
	fn test_include_namespace_sum_type() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceSumType));
	}

	#[test]
	fn test_include_shape_field() {
		assert!(!should_exclude_from_cdc(KeyTag::RowShapeField));
	}

	#[test]
	fn test_include_series() {
		assert!(!should_exclude_from_cdc(KeyTag::Series));
	}

	#[test]
	fn test_include_namespace_series() {
		assert!(!should_exclude_from_cdc(KeyTag::NamespaceSeries));
	}

	#[test]
	fn test_include_series_metadata() {
		assert!(!should_exclude_from_cdc(KeyTag::SeriesMetadata));
	}

	#[test]
	fn test_include_identity() {
		assert!(!should_exclude_from_cdc(KeyTag::Identity));
	}

	#[test]
	fn test_include_role() {
		assert!(!should_exclude_from_cdc(KeyTag::Role));
	}

	#[test]
	fn test_include_granted_role() {
		assert!(!should_exclude_from_cdc(KeyTag::GrantedRole));
	}

	#[test]
	fn test_include_identity_attribute() {
		assert!(!should_exclude_from_cdc(KeyTag::IdentityAttribute));
	}

	#[test]
	fn test_include_identity_attribute_value() {
		assert!(!should_exclude_from_cdc(KeyTag::IdentityAttributeValue));
	}

	#[test]
	fn test_include_authentication() {
		assert!(!should_exclude_from_cdc(KeyTag::Authentication));
	}

	#[test]
	fn test_include_policy() {
		assert!(!should_exclude_from_cdc(KeyTag::Policy));
	}

	#[test]
	fn test_include_policy_op() {
		assert!(!should_exclude_from_cdc(KeyTag::PolicyOp));
	}

	#[test]
	fn test_include_migration() {
		assert!(!should_exclude_from_cdc(KeyTag::Migration));
	}

	#[test]
	fn test_include_migration_event() {
		assert!(!should_exclude_from_cdc(KeyTag::MigrationEvent));
	}

	#[test]
	fn test_exclude_flow_version() {
		assert!(should_exclude_from_cdc(KeyTag::FlowVersion));
	}

	#[test]
	fn test_exclude_config() {
		assert!(should_exclude_from_cdc(KeyTag::ConfigStorage));
	}

	#[test]
	fn test_exclude_version_epoch() {
		assert!(should_exclude_from_cdc(KeyTag::VersionEpoch));
	}
}

impl KeyFields for CdcConsumerKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![Field::BytesDesc(ByteEncoding::Escaped, Cow::Borrowed(self.consumer.as_ref().as_bytes()))]
	}
}
