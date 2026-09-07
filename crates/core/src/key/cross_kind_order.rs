// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::{encoded::EncodedKey, serializer::KeySerializer},
	row::shape::fingerprint::RowShapeFingerprint,
};
use reifydb_runtime::version_epoch::EpochSeconds;
use reifydb_value::value::{
	datetime::DateTime, dictionary::DictionaryId, identity::IdentityId, partition::Partition,
	row_number::RowNumber, sumtype::SumTypeId,
};

use crate::{
	interface::{
		catalog::{
			config::ConfigKey,
			flow::{FlowEdgeId, FlowId, OperatorId},
			id::{
				BindingId, ColumnId, ColumnPropertyId, ColumnSnapshotId, HandlerId, IndexId,
				MigrationEventId, MigrationId, NamespaceId, PrimaryKeyId, ProcedureId, QueueId,
				RelationshipId, RingBufferId, SequenceId, SeriesId, SinkId, SourceId, TableId, ViewId,
			},
			object::ObjectId,
			storage::StorageId,
		},
		cdc::CdcConsumerId,
	},
	key::{
		catalog::{
			BindingKey, ColumnPropertyKey, DictionaryEntryIndexKey, DictionaryEntryKey, DictionaryKey,
			HandlerKey, IndexEntryKey, IndexKey, PrimaryKeyKey, RelationshipKey, SinkKey, SourceKey,
			SumTypeKey, TableKey, VariantHandlerKey, ViewKey,
		},
		cdc::CdcConsumerKey,
		column::{
			ColumnKey, ColumnSequenceKey, ColumnSnapshotKey, ColumnsKey, SeriesColumnSnapshotKey,
			TableColumnSnapshotKey,
		},
		config::ConfigStorageKey,
		flow::{FlowEdgeByFlowKey, FlowEdgeKey, FlowKey, FlowVersionKey},
		identity::{
			AuthenticationKey, GrantedRoleKey, IdentityAttributeKey, IdentityAttributeValueKey,
			IdentityKey, PolicyKey, PolicyOpKey, RoleKey, TokenKey,
		},
		namespace::{
			NamespaceBindingKey, NamespaceDictionaryKey, NamespaceFlowKey, NamespaceHandlerKey,
			NamespaceKey, NamespaceProcedureKey, NamespaceQueueKey, NamespaceRingBufferKey,
			NamespaceSeriesKey, NamespaceSinkKey, NamespaceSourceKey, NamespaceSumTypeKey,
			NamespaceTableKey, NamespaceViewKey,
		},
		operator::{
			key::{OperatorByFlowKey, OperatorKey},
			state::{GroupId, KeyspaceId, OperatorStateKey},
		},
		operator_settings::OperatorSettingsKey,
		output_frontier::OutputFrontierKey,
		partition::PartitionKey,
		procedure::{ProcedureKey, ProcedureParamKey},
		queue::{
			QueueAttemptKey, QueueDeduplicationKey, QueueDueKey, QueueItemStateKey, QueueKey,
			QueueKeyActiveKey, QueuePartitionKey,
		},
		ringbuffer::{RingBufferKey, RingBufferMetadataKey},
		row::{
			PartitionedRowKey, PartitionedSortedViewRowKey, RowKey, RowSequenceKey, RowSettingsKey,
			RowShapeFieldKey, RowShapeKey, SortedViewRowKey,
		},
		series::{PartitionedSeriesRowKey, SeriesKey, SeriesMetadataKey, SeriesRowKey},
		system::{
			MigrationEventKey, MigrationKey, SystemSequenceKey, SystemVersion, SystemVersionKey,
			TransactionVersionKey, VersionEpochKey,
		},
		tag::KeyTag,
	},
	value::index::encoded::EncodedIndexKey,
};

const DECLARED_KINDS: usize = 87;

fn bare(kind: KeyTag) -> EncodedKey {
	let mut serializer = KeySerializer::with_capacity(1);
	serializer.extend_u8(kind as u8);
	serializer.to_encoded_key()
}

fn representative(kind: KeyTag) -> EncodedKey {
	match kind {
		KeyTag::Namespace => NamespaceKey {
			namespace: NamespaceId(1),
		}
		.encode(),
		KeyTag::Table => TableKey {
			table: TableId(1),
		}
		.encode(),
		KeyTag::Row => RowKey {
			storage: StorageId::table(TableId(1)),
			row: RowNumber(1),
		}
		.encode(),
		KeyTag::NamespaceTable => NamespaceTableKey {
			namespace: NamespaceId(1),
			table: TableId(1),
		}
		.encode(),
		KeyTag::SystemSequence => SystemSequenceKey {
			sequence: SequenceId(1),
		}
		.encode(),
		KeyTag::Columns => ColumnsKey {
			column: ColumnId(1),
		}
		.encode(),
		KeyTag::Column => ColumnKey {
			object: ObjectId::table(TableId(1)),
			column: ColumnId(1),
		}
		.encode(),
		KeyTag::RowSequence => RowSequenceKey {
			storage: StorageId::table(TableId(1)),
		}
		.encode(),
		KeyTag::ColumnProperty => ColumnPropertyKey {
			column: ColumnId(1),
			property: ColumnPropertyId(1),
		}
		.encode(),
		KeyTag::SystemVersion => SystemVersionKey {
			version: SystemVersion::Storage,
		}
		.encode(),
		KeyTag::TransactionVersion => TransactionVersionKey {}.encode(),
		KeyTag::Index => IndexKey {
			object: ObjectId::table(TableId(1)),
			index: IndexId::primary(PrimaryKeyId(1)),
		}
		.encode(),
		KeyTag::IndexEntry => IndexEntryKey {
			object: ObjectId::table(TableId(1)),
			index: IndexId::primary(PrimaryKeyId(1)),
			key: EncodedIndexKey::new([0x01]),
		}
		.encode(),
		KeyTag::ColumnSequence => ColumnSequenceKey {
			object: ObjectId::table(TableId(1)),
			column: ColumnId(1),
		}
		.encode(),
		KeyTag::CdcConsumer => CdcConsumerKey {
			consumer: CdcConsumerId::new("probe"),
		}
		.encode(),
		KeyTag::View => ViewKey {
			view: ViewId(1),
		}
		.encode(),
		KeyTag::NamespaceView => NamespaceViewKey {
			namespace: NamespaceId(1),
			view: ViewId(1),
		}
		.encode(),
		KeyTag::PrimaryKey => PrimaryKeyKey {
			primary_key: PrimaryKeyId(1),
		}
		.encode(),
		KeyTag::OperatorState => OperatorStateKey {
			operator: OperatorId(1),
			group: GroupId::ROOT,
			keyspace: KeyspaceId::CUSTOM_NOT_CACHED,
			suffix: vec![1],
		}
		.encode(),
		KeyTag::RingBuffer => RingBufferKey {
			ringbuffer: RingBufferId(1),
		}
		.encode(),
		KeyTag::NamespaceRingBuffer => NamespaceRingBufferKey {
			namespace: NamespaceId(1),
			ringbuffer: RingBufferId(1),
		}
		.encode(),
		KeyTag::RingBufferMetadata => RingBufferMetadataKey {
			storage: StorageId::ringbuffer(RingBufferId(1)),
			partition_values: vec![],
		}
		.encode(),
		KeyTag::Flow => FlowKey {
			flow: FlowId(1),
		}
		.encode(),
		KeyTag::NamespaceFlow => NamespaceFlowKey {
			namespace: NamespaceId(1),
			flow: FlowId(1),
		}
		.encode(),
		KeyTag::Operator => OperatorKey {
			operator: OperatorId(1),
		}
		.encode(),
		KeyTag::OperatorByFlow => OperatorByFlowKey {
			flow: FlowId(1),
			operator: OperatorId(1),
		}
		.encode(),
		KeyTag::FlowEdge => FlowEdgeKey {
			edge: FlowEdgeId(1),
		}
		.encode(),
		KeyTag::FlowEdgeByFlow => FlowEdgeByFlowKey {
			flow: FlowId(1),
			edge: FlowEdgeId(1),
		}
		.encode(),
		KeyTag::OutputFrontier => OutputFrontierKey {
			object: ObjectId::table(TableId(1)),
		}
		.encode(),
		KeyTag::Dictionary => DictionaryKey {
			dictionary: DictionaryId(1),
		}
		.encode(),
		KeyTag::DictionaryEntry => DictionaryEntryKey {
			dictionary: DictionaryId(1),
			hash: [0u8; 16],
		}
		.encode(),
		KeyTag::DictionaryEntryIndex => DictionaryEntryIndexKey {
			dictionary: DictionaryId(1),
			id: 1,
		}
		.encode(),
		KeyTag::NamespaceDictionary => NamespaceDictionaryKey {
			namespace: NamespaceId(1),
			dictionary: DictionaryId(1),
		}
		.encode(),
		KeyTag::Metric => bare(kind),
		KeyTag::FlowVersion => FlowVersionKey {
			flow: FlowId(1),
		}
		.encode(),
		KeyTag::RowShape => RowShapeKey {
			fingerprint: RowShapeFingerprint::new(1),
		}
		.encode(),
		KeyTag::RowShapeField => RowShapeFieldKey {
			shape_fingerprint: RowShapeFingerprint::new(1),
			field_index: 0,
		}
		.encode(),
		KeyTag::SumType => SumTypeKey {
			sumtype: SumTypeId(1),
		}
		.encode(),
		KeyTag::NamespaceSumType => NamespaceSumTypeKey {
			namespace: NamespaceId(1),
			sumtype: SumTypeId(1),
		}
		.encode(),
		KeyTag::Handler => HandlerKey {
			handler: HandlerId(1),
		}
		.encode(),
		KeyTag::NamespaceHandler => NamespaceHandlerKey {
			namespace: NamespaceId(1),
			handler: HandlerId(1),
		}
		.encode(),
		KeyTag::VariantHandler => VariantHandlerKey {
			namespace: NamespaceId(1),
			sumtype: SumTypeId(1),
			variant_tag: 0,
			handler: HandlerId(1),
		}
		.encode(),
		KeyTag::Series => SeriesKey {
			series: SeriesId(1),
		}
		.encode(),
		KeyTag::NamespaceSeries => NamespaceSeriesKey {
			namespace: NamespaceId(1),
			series: SeriesId(1),
		}
		.encode(),
		KeyTag::SeriesMetadata => SeriesMetadataKey {
			storage: StorageId::series(SeriesId(1)),
		}
		.encode(),
		KeyTag::Identity => IdentityKey {
			identity: IdentityId::root(),
		}
		.encode(),
		KeyTag::Role => RoleKey {
			role: 1,
		}
		.encode(),
		KeyTag::GrantedRole => GrantedRoleKey {
			identity: IdentityId::root(),
			role: 1,
		}
		.encode(),
		KeyTag::Policy => PolicyKey {
			policy: 1,
		}
		.encode(),
		KeyTag::PolicyOp => PolicyOpKey {
			policy: 1,
			op_index: 0,
		}
		.encode(),
		KeyTag::Migration => MigrationKey {
			migration: MigrationId(1),
		}
		.encode(),
		KeyTag::MigrationEvent => MigrationEventKey {
			event: MigrationEventId(1),
		}
		.encode(),
		KeyTag::Authentication => AuthenticationKey {
			authentication: 1,
		}
		.encode(),
		KeyTag::ConfigStorage => ConfigStorageKey {
			key: ConfigKey::OracleWindowSize,
		}
		.encode(),
		KeyTag::Token => TokenKey {
			token: 1,
		}
		.encode(),
		KeyTag::Source => SourceKey {
			source: SourceId(1),
		}
		.encode(),
		KeyTag::NamespaceSource => NamespaceSourceKey {
			namespace: NamespaceId(1),
			source: SourceId(1),
		}
		.encode(),
		KeyTag::Sink => SinkKey {
			sink: SinkId(1),
		}
		.encode(),
		KeyTag::NamespaceSink => NamespaceSinkKey {
			namespace: NamespaceId(1),
			sink: SinkId(1),
		}
		.encode(),
		KeyTag::RowSettings => RowSettingsKey {
			storage: StorageId::table(TableId(1)),
		}
		.encode(),
		KeyTag::Procedure => ProcedureKey {
			procedure: ProcedureId::from_raw(1),
		}
		.encode(),
		KeyTag::NamespaceProcedure => NamespaceProcedureKey {
			namespace: NamespaceId(1),
			procedure: ProcedureId::from_raw(1),
		}
		.encode(),
		KeyTag::ProcedureParam => ProcedureParamKey {
			procedure: ProcedureId::from_raw(1),
			param_index: 0,
		}
		.encode(),
		KeyTag::Binding => BindingKey {
			binding: BindingId(1),
		}
		.encode(),
		KeyTag::NamespaceBinding => NamespaceBindingKey {
			namespace: NamespaceId(1),
			binding: BindingId(1),
		}
		.encode(),
		KeyTag::OperatorSettings => OperatorSettingsKey {
			operator: OperatorId(1),
		}
		.encode(),
		KeyTag::ColumnSnapshot => ColumnSnapshotKey {
			snapshot: ColumnSnapshotId(1),
		}
		.encode(),
		KeyTag::SeriesColumnSnapshot => SeriesColumnSnapshotKey {
			series: SeriesId(1),
			snapshot: ColumnSnapshotId(1),
		}
		.encode(),
		KeyTag::TableColumnSnapshot => TableColumnSnapshotKey {
			table: TableId(1),
			snapshot: ColumnSnapshotId(1),
		}
		.encode(),
		KeyTag::VersionEpoch => VersionEpochKey {
			bucket: EpochSeconds::new(1),
		}
		.encode(),
		KeyTag::IdentityAttribute => IdentityAttributeKey {
			attribute: 1,
		}
		.encode(),
		KeyTag::IdentityAttributeValue => IdentityAttributeValueKey {
			identity: IdentityId::root(),
			attribute: 1,
		}
		.encode(),
		KeyTag::PartitionedRow => PartitionedRowKey {
			storage: StorageId::table(TableId(1)),
			partition: Partition(1),
			row: RowNumber(1),
		}
		.encode(),
		KeyTag::Partition => PartitionKey {
			object: ObjectId::table(TableId(1)),
			partition: Partition(1),
		}
		.encode(),
		KeyTag::Queue => QueueKey {
			queue: QueueId(1),
		}
		.encode(),
		KeyTag::NamespaceQueue => NamespaceQueueKey {
			namespace: NamespaceId(1),
			queue: QueueId(1),
		}
		.encode(),
		KeyTag::QueueDeduplication => QueueDeduplicationKey::new(QueueId(1), b"probe").encode(),
		KeyTag::Relationship => RelationshipKey {
			relationship: RelationshipId(1),
		}
		.encode(),
		KeyTag::SeriesRow => SeriesRowKey {
			storage: StorageId::series(SeriesId(1)),
			variant_tag: None,
			key: 1,
			sequence: 1,
		}
		.encode(),
		KeyTag::PartitionedSeriesRow => PartitionedSeriesRowKey {
			storage: StorageId::series(SeriesId(1)),
			partition: Partition(1),
			variant_tag: None,
			key: 1,
			sequence: 1,
		}
		.encode(),
		KeyTag::QueuePartition => QueuePartitionKey {
			queue: QueueId(1),
			partition: 0,
		}
		.encode(),
		KeyTag::QueueItemState => QueueItemStateKey {
			queue: QueueId(1),
			partition: 0,
			row: RowNumber(1),
		}
		.encode(),
		KeyTag::QueueDue => QueueDueKey {
			queue: QueueId(1),
			partition: 0,
			due: DateTime::from_bits(1),
			row: RowNumber(1),
		}
		.encode(),
		KeyTag::QueueAttempt => QueueAttemptKey {
			queue: QueueId(1),
			row: RowNumber(1),
			attempt: 0,
		}
		.encode(),
		KeyTag::QueueKeyActive => QueueKeyActiveKey {
			queue: QueueId(1),
			partition: 0,
			key_hash: 1,
			row: RowNumber(1),
		}
		.encode(),
		KeyTag::SortedViewRow => SortedViewRowKey::storage_start(StorageId::view(ViewId(1))),
		KeyTag::PartitionedSortedViewRow => {
			PartitionedSortedViewRowKey::storage_start(StorageId::view(ViewId(1)))
		}
	}
}

fn all_kinds() -> Vec<KeyTag> {
	(0x00u8..=0xFF).filter_map(|byte| KeyTag::try_from(byte).ok()).collect()
}

#[test]
fn every_declared_kind_is_reachable_from_a_byte() {
	let kinds = all_kinds();
	assert_eq!(
		kinds.len(),
		DECLARED_KINDS,
		"KeyTag::try_from accepts {} discriminants but {DECLARED_KINDS} are declared; add the new \
		 variant to try_from and bump DECLARED_KINDS",
		kinds.len()
	);
}

#[test]
fn each_representative_opens_with_its_own_inverted_kind_byte() {
	for kind in all_kinds() {
		let encoded = representative(kind);
		assert_eq!(
			encoded.as_slice().first().copied(),
			Some(!(kind as u8)),
			"{kind:?} must encode its kind byte inverted"
		);
	}
}

#[test]
fn encoded_keys_sort_descending_by_kind_discriminant() {
	let kinds = all_kinds();

	let mut encoded: Vec<(KeyTag, Vec<u8>)> =
		kinds.iter().map(|kind| (*kind, representative(*kind).as_slice().to_vec())).collect();
	encoded.sort_by(|left, right| left.1.cmp(&right.1));

	let sorted: Vec<u8> = encoded.iter().map(|(kind, _)| *kind as u8).collect();

	let mut expected: Vec<u8> = kinds.iter().map(|kind| *kind as u8).collect();
	expected.sort_unstable();
	expected.reverse();

	assert_eq!(
		sorted, expected,
		"encoded keys must sort descending by KeyTag discriminant, highest discriminant first"
	);
}

#[test]
fn a_higher_kind_discriminant_sorts_strictly_before_a_lower_one() {
	let kinds = all_kinds();

	for pair in kinds.windows(2) {
		let lower = representative(pair[0]);
		let higher = representative(pair[1]);
		assert!(
			higher.as_slice() < lower.as_slice(),
			"{:?} (0x{:02X}) must sort before {:?} (0x{:02X}) in byte order",
			pair[1],
			pair[1] as u8,
			pair[0],
			pair[0] as u8
		);
	}
}

#[test]
fn row_sorts_before_table_because_the_kind_byte_is_inverted() {
	let row = representative(KeyTag::Row);
	let table = representative(KeyTag::Table);

	assert!(KeyTag::Row as u8 > KeyTag::Table as u8);
	assert!(row.as_slice() < table.as_slice(), "Row must sort before Table despite its larger discriminant");
}
