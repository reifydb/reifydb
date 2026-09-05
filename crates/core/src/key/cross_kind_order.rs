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
		EncodableKey,
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
		kind::KeyKind,
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
		typed::key::Key,
	},
	value::index::encoded::EncodedIndexKey,
};

const DECLARED_KINDS: usize = 87;

fn bare(kind: KeyKind) -> EncodedKey {
	let mut serializer = KeySerializer::with_capacity(1);
	serializer.extend_u8(kind as u8);
	serializer.to_encoded_key()
}

fn representative(kind: KeyKind) -> EncodedKey {
	match kind {
		KeyKind::Namespace => NamespaceKey {
			namespace: NamespaceId(1),
		}
		.encode(),
		KeyKind::Table => TableKey {
			table: TableId(1),
		}
		.encode(),
		KeyKind::Row => RowKey {
			storage: StorageId::table(TableId(1)),
			row: RowNumber(1),
		}
		.encode(),
		KeyKind::NamespaceTable => NamespaceTableKey {
			namespace: NamespaceId(1),
			table: TableId(1),
		}
		.encode(),
		KeyKind::SystemSequence => SystemSequenceKey {
			sequence: SequenceId(1),
		}
		.encode(),
		KeyKind::Columns => ColumnsKey {
			column: ColumnId(1),
		}
		.encode(),
		KeyKind::Column => ColumnKey {
			object: ObjectId::table(TableId(1)),
			column: ColumnId(1),
		}
		.encode(),
		KeyKind::RowSequence => RowSequenceKey {
			storage: StorageId::table(TableId(1)),
		}
		.encode(),
		KeyKind::ColumnProperty => ColumnPropertyKey {
			column: ColumnId(1),
			property: ColumnPropertyId(1),
		}
		.encode(),
		KeyKind::SystemVersion => SystemVersionKey {
			version: SystemVersion::Storage,
		}
		.encode(),
		KeyKind::TransactionVersion => TransactionVersionKey {}.encode(),
		KeyKind::Index => IndexKey {
			object: ObjectId::table(TableId(1)),
			index: IndexId::primary(PrimaryKeyId(1)),
		}
		.encode(),
		KeyKind::IndexEntry => IndexEntryKey {
			object: ObjectId::table(TableId(1)),
			index: IndexId::primary(PrimaryKeyId(1)),
			key: EncodedIndexKey::new([0x01]),
		}
		.encode(),
		KeyKind::ColumnSequence => ColumnSequenceKey {
			object: ObjectId::table(TableId(1)),
			column: ColumnId(1),
		}
		.encode(),
		KeyKind::CdcConsumer => CdcConsumerKey {
			consumer: CdcConsumerId::new("probe"),
		}
		.encode(),
		KeyKind::View => ViewKey {
			view: ViewId(1),
		}
		.encode(),
		KeyKind::NamespaceView => NamespaceViewKey {
			namespace: NamespaceId(1),
			view: ViewId(1),
		}
		.encode(),
		KeyKind::PrimaryKey => PrimaryKeyKey {
			primary_key: PrimaryKeyId(1),
		}
		.encode(),
		KeyKind::OperatorState => OperatorStateKey {
			operator: OperatorId(1),
			group: GroupId::ROOT,
			keyspace: KeyspaceId::CUSTOM_NOT_CACHED,
			suffix: vec![1],
		}
		.encode(),
		KeyKind::RingBuffer => RingBufferKey {
			ringbuffer: RingBufferId(1),
		}
		.encode(),
		KeyKind::NamespaceRingBuffer => NamespaceRingBufferKey {
			namespace: NamespaceId(1),
			ringbuffer: RingBufferId(1),
		}
		.encode(),
		KeyKind::RingBufferMetadata => RingBufferMetadataKey {
			storage: StorageId::ringbuffer(RingBufferId(1)),
			partition_values: vec![],
		}
		.encode(),
		KeyKind::Flow => FlowKey {
			flow: FlowId(1),
		}
		.encode(),
		KeyKind::NamespaceFlow => NamespaceFlowKey {
			namespace: NamespaceId(1),
			flow: FlowId(1),
		}
		.encode(),
		KeyKind::Operator => OperatorKey {
			operator: OperatorId(1),
		}
		.encode(),
		KeyKind::OperatorByFlow => OperatorByFlowKey {
			flow: FlowId(1),
			operator: OperatorId(1),
		}
		.encode(),
		KeyKind::FlowEdge => FlowEdgeKey {
			edge: FlowEdgeId(1),
		}
		.encode(),
		KeyKind::FlowEdgeByFlow => FlowEdgeByFlowKey {
			flow: FlowId(1),
			edge: FlowEdgeId(1),
		}
		.encode(),
		KeyKind::OutputFrontier => OutputFrontierKey {
			object: ObjectId::table(TableId(1)),
		}
		.encode(),
		KeyKind::Dictionary => DictionaryKey {
			dictionary: DictionaryId(1),
		}
		.encode(),
		KeyKind::DictionaryEntry => DictionaryEntryKey {
			dictionary: DictionaryId(1),
			hash: [0u8; 16],
		}
		.encode(),
		KeyKind::DictionaryEntryIndex => DictionaryEntryIndexKey {
			dictionary: DictionaryId(1),
			id: 1,
		}
		.encode(),
		KeyKind::NamespaceDictionary => NamespaceDictionaryKey {
			namespace: NamespaceId(1),
			dictionary: DictionaryId(1),
		}
		.encode(),
		KeyKind::Metric => bare(kind),
		KeyKind::FlowVersion => FlowVersionKey {
			flow: FlowId(1),
		}
		.encode(),
		KeyKind::RowShape => RowShapeKey {
			fingerprint: RowShapeFingerprint::new(1),
		}
		.encode(),
		KeyKind::RowShapeField => RowShapeFieldKey {
			shape_fingerprint: RowShapeFingerprint::new(1),
			field_index: 0,
		}
		.encode(),
		KeyKind::SumType => SumTypeKey {
			sumtype: SumTypeId(1),
		}
		.encode(),
		KeyKind::NamespaceSumType => NamespaceSumTypeKey {
			namespace: NamespaceId(1),
			sumtype: SumTypeId(1),
		}
		.encode(),
		KeyKind::Handler => HandlerKey {
			handler: HandlerId(1),
		}
		.encode(),
		KeyKind::NamespaceHandler => NamespaceHandlerKey {
			namespace: NamespaceId(1),
			handler: HandlerId(1),
		}
		.encode(),
		KeyKind::VariantHandler => VariantHandlerKey {
			namespace: NamespaceId(1),
			sumtype: SumTypeId(1),
			variant_tag: 0,
			handler: HandlerId(1),
		}
		.encode(),
		KeyKind::Series => SeriesKey {
			series: SeriesId(1),
		}
		.encode(),
		KeyKind::NamespaceSeries => NamespaceSeriesKey {
			namespace: NamespaceId(1),
			series: SeriesId(1),
		}
		.encode(),
		KeyKind::SeriesMetadata => SeriesMetadataKey {
			storage: StorageId::series(SeriesId(1)),
		}
		.encode(),
		KeyKind::Identity => IdentityKey {
			identity: IdentityId::root(),
		}
		.encode(),
		KeyKind::Role => RoleKey {
			role: 1,
		}
		.encode(),
		KeyKind::GrantedRole => GrantedRoleKey {
			identity: IdentityId::root(),
			role: 1,
		}
		.encode(),
		KeyKind::Policy => PolicyKey {
			policy: 1,
		}
		.encode(),
		KeyKind::PolicyOp => PolicyOpKey {
			policy: 1,
			op_index: 0,
		}
		.encode(),
		KeyKind::Migration => MigrationKey {
			migration: MigrationId(1),
		}
		.encode(),
		KeyKind::MigrationEvent => MigrationEventKey {
			event: MigrationEventId(1),
		}
		.encode(),
		KeyKind::Authentication => AuthenticationKey {
			authentication: 1,
		}
		.encode(),
		KeyKind::ConfigStorage => ConfigStorageKey {
			key: ConfigKey::OracleWindowSize,
		}
		.encode(),
		KeyKind::Token => TokenKey {
			token: 1,
		}
		.encode(),
		KeyKind::Source => SourceKey {
			source: SourceId(1),
		}
		.encode(),
		KeyKind::NamespaceSource => NamespaceSourceKey {
			namespace: NamespaceId(1),
			source: SourceId(1),
		}
		.encode(),
		KeyKind::Sink => SinkKey {
			sink: SinkId(1),
		}
		.encode(),
		KeyKind::NamespaceSink => NamespaceSinkKey {
			namespace: NamespaceId(1),
			sink: SinkId(1),
		}
		.encode(),
		KeyKind::RowSettings => RowSettingsKey {
			storage: StorageId::table(TableId(1)),
		}
		.encode(),
		KeyKind::Procedure => ProcedureKey {
			procedure: ProcedureId::from_raw(1),
		}
		.encode(),
		KeyKind::NamespaceProcedure => NamespaceProcedureKey {
			namespace: NamespaceId(1),
			procedure: ProcedureId::from_raw(1),
		}
		.encode(),
		KeyKind::ProcedureParam => ProcedureParamKey {
			procedure: ProcedureId::from_raw(1),
			param_index: 0,
		}
		.encode(),
		KeyKind::Binding => BindingKey {
			binding: BindingId(1),
		}
		.encode(),
		KeyKind::NamespaceBinding => NamespaceBindingKey {
			namespace: NamespaceId(1),
			binding: BindingId(1),
		}
		.encode(),
		KeyKind::OperatorSettings => OperatorSettingsKey {
			operator: OperatorId(1),
		}
		.encode(),
		KeyKind::ColumnSnapshot => ColumnSnapshotKey {
			snapshot: ColumnSnapshotId(1),
		}
		.encode(),
		KeyKind::SeriesColumnSnapshot => SeriesColumnSnapshotKey {
			series: SeriesId(1),
			snapshot: ColumnSnapshotId(1),
		}
		.encode(),
		KeyKind::TableColumnSnapshot => TableColumnSnapshotKey {
			table: TableId(1),
			snapshot: ColumnSnapshotId(1),
		}
		.encode(),
		KeyKind::VersionEpoch => VersionEpochKey {
			bucket: EpochSeconds::new(1),
		}
		.encode(),
		KeyKind::IdentityAttribute => IdentityAttributeKey {
			attribute: 1,
		}
		.encode(),
		KeyKind::IdentityAttributeValue => IdentityAttributeValueKey {
			identity: IdentityId::root(),
			attribute: 1,
		}
		.encode(),
		KeyKind::PartitionedRow => PartitionedRowKey {
			storage: StorageId::table(TableId(1)),
			partition: Partition(1),
			row: RowNumber(1),
		}
		.encode(),
		KeyKind::Partition => PartitionKey {
			object: ObjectId::table(TableId(1)),
			partition: Partition(1),
		}
		.encode(),
		KeyKind::Queue => QueueKey {
			queue: QueueId(1),
		}
		.encode(),
		KeyKind::NamespaceQueue => NamespaceQueueKey {
			namespace: NamespaceId(1),
			queue: QueueId(1),
		}
		.encode(),
		KeyKind::QueueDeduplication => QueueDeduplicationKey::new(QueueId(1), b"probe").encode(),
		KeyKind::Relationship => RelationshipKey {
			relationship: RelationshipId(1),
		}
		.encode(),
		KeyKind::SeriesRow => SeriesRowKey {
			storage: StorageId::series(SeriesId(1)),
			variant_tag: None,
			key: 1,
			sequence: 1,
		}
		.encode(),
		KeyKind::PartitionedSeriesRow => PartitionedSeriesRowKey {
			storage: StorageId::series(SeriesId(1)),
			partition: Partition(1),
			variant_tag: None,
			key: 1,
			sequence: 1,
		}
		.encode(),
		KeyKind::QueuePartition => QueuePartitionKey {
			queue: QueueId(1),
			partition: 0,
		}
		.encode(),
		KeyKind::QueueItemState => QueueItemStateKey {
			queue: QueueId(1),
			partition: 0,
			row: RowNumber(1),
		}
		.encode(),
		KeyKind::QueueDue => QueueDueKey {
			queue: QueueId(1),
			partition: 0,
			due: DateTime::from_bits(1),
			row: RowNumber(1),
		}
		.encode(),
		KeyKind::QueueAttempt => QueueAttemptKey {
			queue: QueueId(1),
			row: RowNumber(1),
			attempt: 0,
		}
		.encode(),
		KeyKind::QueueKeyActive => QueueKeyActiveKey {
			queue: QueueId(1),
			partition: 0,
			key_hash: 1,
			row: RowNumber(1),
		}
		.encode(),
		KeyKind::SortedViewRow => SortedViewRowKey::storage_start(StorageId::view(ViewId(1))),
		KeyKind::PartitionedSortedViewRow => {
			PartitionedSortedViewRowKey::storage_start(StorageId::view(ViewId(1)))
		}
	}
}

fn all_kinds() -> Vec<KeyKind> {
	(0x00u8..=0xFF).filter_map(|byte| KeyKind::try_from(byte).ok()).collect()
}

#[test]
fn every_declared_kind_is_reachable_from_a_byte() {
	let kinds = all_kinds();
	assert_eq!(
		kinds.len(),
		DECLARED_KINDS,
		"KeyKind::try_from accepts {} discriminants but {DECLARED_KINDS} are declared; add the new \
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

	let mut encoded: Vec<(KeyKind, Vec<u8>)> =
		kinds.iter().map(|kind| (*kind, representative(*kind).as_slice().to_vec())).collect();
	encoded.sort_by(|left, right| left.1.cmp(&right.1));

	let sorted: Vec<u8> = encoded.iter().map(|(kind, _)| *kind as u8).collect();

	let mut expected: Vec<u8> = kinds.iter().map(|kind| *kind as u8).collect();
	expected.sort_unstable();
	expected.reverse();

	assert_eq!(
		sorted, expected,
		"encoded keys must sort descending by KeyKind discriminant, highest discriminant first"
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
	let row = representative(KeyKind::Row);
	let table = representative(KeyKind::Table);

	assert!(KeyKind::Row as u8 > KeyKind::Table as u8);
	assert!(row.as_slice() < table.as_slice(), "Row must sort before Table despite its larger discriminant");
}
