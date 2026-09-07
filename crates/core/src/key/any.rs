// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, cmp::Ordering};

use reifydb_codec::key::{encode_bytes, encode_u128_varint, encoded::EncodedKey, serializer::KeySerializer};
use reifydb_value::value::Value;
use smallvec::SmallVec;

use crate::{
	interface::{
		catalog::{id::IndexId, metrics::MetricsId, object::ObjectId},
		store::Tier,
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
		metric::{MetricCdcKey, MetricStorageKey},
		namespace::{
			NamespaceBindingKey, NamespaceDictionaryKey, NamespaceFlowKey, NamespaceHandlerKey,
			NamespaceKey, NamespaceProcedureKey, NamespaceQueueKey, NamespaceRingBufferKey,
			NamespaceSeriesKey, NamespaceSinkKey, NamespaceSourceKey, NamespaceSumTypeKey,
			NamespaceTableKey, NamespaceViewKey,
		},
		operator::{
			key::{OperatorByFlowKey, OperatorKey},
			state::OperatorStateKey,
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
			RowShapeFieldKey, RowShapeKey, SortedViewRowKey, encode_sort_run,
		},
		series::{PartitionedSeriesRowKey, SeriesKey, SeriesMetadataKey, SeriesRowKey},
		system::{
			MigrationEventKey, MigrationKey, SystemSequenceKey, SystemVersionKey, TransactionVersionKey,
			VersionEpochKey,
		},
		tag::KeyTag,
	},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKey {
	Cdc(MetricCdcKey),
	Storage(MetricStorageKey),
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TaggedKey {
	Namespace(NamespaceKey),
	Table(TableKey),
	Row(RowKey),
	NamespaceTable(NamespaceTableKey),
	SystemSequence(SystemSequenceKey),
	Columns(ColumnsKey),
	Column(ColumnKey),
	RowSequence(RowSequenceKey),
	ColumnProperty(ColumnPropertyKey),
	SystemVersion(SystemVersionKey),
	TransactionVersion(TransactionVersionKey),
	Index(IndexKey),
	IndexEntry(IndexEntryKey),
	ColumnSequence(ColumnSequenceKey),
	CdcConsumer(CdcConsumerKey),
	View(ViewKey),
	NamespaceView(NamespaceViewKey),
	PrimaryKey(PrimaryKeyKey),
	OperatorState(OperatorStateKey),
	RingBuffer(RingBufferKey),
	NamespaceRingBuffer(NamespaceRingBufferKey),
	RingBufferMetadata(RingBufferMetadataKey),
	Flow(FlowKey),
	NamespaceFlow(NamespaceFlowKey),
	Operator(OperatorKey),
	OperatorByFlow(OperatorByFlowKey),
	FlowEdge(FlowEdgeKey),
	FlowEdgeByFlow(FlowEdgeByFlowKey),
	OutputFrontier(OutputFrontierKey),
	Dictionary(DictionaryKey),
	DictionaryEntry(DictionaryEntryKey),
	DictionaryEntryIndex(DictionaryEntryIndexKey),
	NamespaceDictionary(NamespaceDictionaryKey),
	Metric(MetricKey),
	FlowVersion(FlowVersionKey),
	RowShape(RowShapeKey),
	RowShapeField(RowShapeFieldKey),
	SumType(SumTypeKey),
	NamespaceSumType(NamespaceSumTypeKey),
	Handler(HandlerKey),
	NamespaceHandler(NamespaceHandlerKey),
	VariantHandler(VariantHandlerKey),
	Series(SeriesKey),
	NamespaceSeries(NamespaceSeriesKey),
	SeriesMetadata(SeriesMetadataKey),
	Identity(IdentityKey),
	Role(RoleKey),
	GrantedRole(GrantedRoleKey),
	Policy(PolicyKey),
	PolicyOp(PolicyOpKey),
	Migration(MigrationKey),
	MigrationEvent(MigrationEventKey),
	Authentication(AuthenticationKey),
	ConfigStorage(ConfigStorageKey),
	Token(TokenKey),
	Source(SourceKey),
	NamespaceSource(NamespaceSourceKey),
	Sink(SinkKey),
	NamespaceSink(NamespaceSinkKey),
	RowSettings(RowSettingsKey),
	Procedure(ProcedureKey),
	NamespaceProcedure(NamespaceProcedureKey),
	ProcedureParam(ProcedureParamKey),
	Binding(BindingKey),
	NamespaceBinding(NamespaceBindingKey),
	OperatorSettings(OperatorSettingsKey),
	ColumnSnapshot(ColumnSnapshotKey),
	SeriesColumnSnapshot(SeriesColumnSnapshotKey),
	TableColumnSnapshot(TableColumnSnapshotKey),
	VersionEpoch(VersionEpochKey),
	IdentityAttribute(IdentityAttributeKey),
	IdentityAttributeValue(IdentityAttributeValueKey),
	PartitionedRow(PartitionedRowKey),
	Partition(PartitionKey),
	Queue(QueueKey),
	NamespaceQueue(NamespaceQueueKey),
	QueueDeduplication(QueueDeduplicationKey),
	Relationship(RelationshipKey),
	SeriesRow(SeriesRowKey),
	PartitionedSeriesRow(PartitionedSeriesRowKey),
	QueuePartition(QueuePartitionKey),
	QueueItemState(QueueItemStateKey),
	QueueDue(QueueDueKey),
	QueueAttempt(QueueAttemptKey),
	QueueKeyActive(QueueKeyActiveKey),
	SortedViewRow(SortedViewRowKey),
	PartitionedSortedViewRow(PartitionedSortedViewRowKey),
}

impl TaggedKey {
	pub fn kind(&self) -> KeyTag {
		match self {
			Self::Namespace(_) => KeyTag::Namespace,
			Self::Table(_) => KeyTag::Table,
			Self::Row(_) => KeyTag::Row,
			Self::NamespaceTable(_) => KeyTag::NamespaceTable,
			Self::SystemSequence(_) => KeyTag::SystemSequence,
			Self::Columns(_) => KeyTag::Columns,
			Self::Column(_) => KeyTag::Column,
			Self::RowSequence(_) => KeyTag::RowSequence,
			Self::ColumnProperty(_) => KeyTag::ColumnProperty,
			Self::SystemVersion(_) => KeyTag::SystemVersion,
			Self::TransactionVersion(_) => KeyTag::TransactionVersion,
			Self::Index(_) => KeyTag::Index,
			Self::IndexEntry(_) => KeyTag::IndexEntry,
			Self::ColumnSequence(_) => KeyTag::ColumnSequence,
			Self::CdcConsumer(_) => KeyTag::CdcConsumer,
			Self::View(_) => KeyTag::View,
			Self::NamespaceView(_) => KeyTag::NamespaceView,
			Self::PrimaryKey(_) => KeyTag::PrimaryKey,
			Self::OperatorState(_) => KeyTag::OperatorState,
			Self::RingBuffer(_) => KeyTag::RingBuffer,
			Self::NamespaceRingBuffer(_) => KeyTag::NamespaceRingBuffer,
			Self::RingBufferMetadata(_) => KeyTag::RingBufferMetadata,
			Self::Flow(_) => KeyTag::Flow,
			Self::NamespaceFlow(_) => KeyTag::NamespaceFlow,
			Self::Operator(_) => KeyTag::Operator,
			Self::OperatorByFlow(_) => KeyTag::OperatorByFlow,
			Self::FlowEdge(_) => KeyTag::FlowEdge,
			Self::FlowEdgeByFlow(_) => KeyTag::FlowEdgeByFlow,
			Self::OutputFrontier(_) => KeyTag::OutputFrontier,
			Self::Dictionary(_) => KeyTag::Dictionary,
			Self::DictionaryEntry(_) => KeyTag::DictionaryEntry,
			Self::DictionaryEntryIndex(_) => KeyTag::DictionaryEntryIndex,
			Self::NamespaceDictionary(_) => KeyTag::NamespaceDictionary,
			Self::Metric(_) => KeyTag::Metric,
			Self::FlowVersion(_) => KeyTag::FlowVersion,
			Self::RowShape(_) => KeyTag::RowShape,
			Self::RowShapeField(_) => KeyTag::RowShapeField,
			Self::SumType(_) => KeyTag::SumType,
			Self::NamespaceSumType(_) => KeyTag::NamespaceSumType,
			Self::Handler(_) => KeyTag::Handler,
			Self::NamespaceHandler(_) => KeyTag::NamespaceHandler,
			Self::VariantHandler(_) => KeyTag::VariantHandler,
			Self::Series(_) => KeyTag::Series,
			Self::NamespaceSeries(_) => KeyTag::NamespaceSeries,
			Self::SeriesMetadata(_) => KeyTag::SeriesMetadata,
			Self::Identity(_) => KeyTag::Identity,
			Self::Role(_) => KeyTag::Role,
			Self::GrantedRole(_) => KeyTag::GrantedRole,
			Self::Policy(_) => KeyTag::Policy,
			Self::PolicyOp(_) => KeyTag::PolicyOp,
			Self::Migration(_) => KeyTag::Migration,
			Self::MigrationEvent(_) => KeyTag::MigrationEvent,
			Self::Authentication(_) => KeyTag::Authentication,
			Self::ConfigStorage(_) => KeyTag::ConfigStorage,
			Self::Token(_) => KeyTag::Token,
			Self::Source(_) => KeyTag::Source,
			Self::NamespaceSource(_) => KeyTag::NamespaceSource,
			Self::Sink(_) => KeyTag::Sink,
			Self::NamespaceSink(_) => KeyTag::NamespaceSink,
			Self::RowSettings(_) => KeyTag::RowSettings,
			Self::Procedure(_) => KeyTag::Procedure,
			Self::NamespaceProcedure(_) => KeyTag::NamespaceProcedure,
			Self::ProcedureParam(_) => KeyTag::ProcedureParam,
			Self::Binding(_) => KeyTag::Binding,
			Self::NamespaceBinding(_) => KeyTag::NamespaceBinding,
			Self::OperatorSettings(_) => KeyTag::OperatorSettings,
			Self::ColumnSnapshot(_) => KeyTag::ColumnSnapshot,
			Self::SeriesColumnSnapshot(_) => KeyTag::SeriesColumnSnapshot,
			Self::TableColumnSnapshot(_) => KeyTag::TableColumnSnapshot,
			Self::VersionEpoch(_) => KeyTag::VersionEpoch,
			Self::IdentityAttribute(_) => KeyTag::IdentityAttribute,
			Self::IdentityAttributeValue(_) => KeyTag::IdentityAttributeValue,
			Self::PartitionedRow(_) => KeyTag::PartitionedRow,
			Self::Partition(_) => KeyTag::Partition,
			Self::Queue(_) => KeyTag::Queue,
			Self::NamespaceQueue(_) => KeyTag::NamespaceQueue,
			Self::QueueDeduplication(_) => KeyTag::QueueDeduplication,
			Self::Relationship(_) => KeyTag::Relationship,
			Self::SeriesRow(_) => KeyTag::SeriesRow,
			Self::PartitionedSeriesRow(_) => KeyTag::PartitionedSeriesRow,
			Self::QueuePartition(_) => KeyTag::QueuePartition,
			Self::QueueItemState(_) => KeyTag::QueueItemState,
			Self::QueueDue(_) => KeyTag::QueueDue,
			Self::QueueAttempt(_) => KeyTag::QueueAttempt,
			Self::QueueKeyActive(_) => KeyTag::QueueKeyActive,
			Self::SortedViewRow(_) => KeyTag::SortedViewRow,
			Self::PartitionedSortedViewRow(_) => KeyTag::PartitionedSortedViewRow,
		}
	}

	pub fn encode(&self) -> EncodedKey {
		match self {
			Self::Namespace(key) => key.encode(),
			Self::Table(key) => key.encode(),
			Self::Row(key) => key.encode(),
			Self::NamespaceTable(key) => key.encode(),
			Self::SystemSequence(key) => key.encode(),
			Self::Columns(key) => key.encode(),
			Self::Column(key) => key.encode(),
			Self::RowSequence(key) => key.encode(),
			Self::ColumnProperty(key) => key.encode(),
			Self::SystemVersion(key) => key.encode(),
			Self::TransactionVersion(key) => key.encode(),
			Self::Index(key) => key.encode(),
			Self::IndexEntry(key) => key.encode(),
			Self::ColumnSequence(key) => key.encode(),
			Self::CdcConsumer(key) => key.encode(),
			Self::View(key) => key.encode(),
			Self::NamespaceView(key) => key.encode(),
			Self::PrimaryKey(key) => key.encode(),
			Self::OperatorState(key) => key.encode(),
			Self::RingBuffer(key) => key.encode(),
			Self::NamespaceRingBuffer(key) => key.encode(),
			Self::RingBufferMetadata(key) => key.encode(),
			Self::Flow(key) => key.encode(),
			Self::NamespaceFlow(key) => key.encode(),
			Self::Operator(key) => key.encode(),
			Self::OperatorByFlow(key) => key.encode(),
			Self::FlowEdge(key) => key.encode(),
			Self::FlowEdgeByFlow(key) => key.encode(),
			Self::OutputFrontier(key) => key.encode(),
			Self::Dictionary(key) => key.encode(),
			Self::DictionaryEntry(key) => key.encode(),
			Self::DictionaryEntryIndex(key) => key.encode(),
			Self::NamespaceDictionary(key) => key.encode(),
			Self::Metric(MetricKey::Cdc(key)) => key.encode(),
			Self::Metric(MetricKey::Storage(key)) => key.encode(),
			Self::FlowVersion(key) => key.encode(),
			Self::RowShape(key) => key.encode(),
			Self::RowShapeField(key) => key.encode(),
			Self::SumType(key) => key.encode(),
			Self::NamespaceSumType(key) => key.encode(),
			Self::Handler(key) => key.encode(),
			Self::NamespaceHandler(key) => key.encode(),
			Self::VariantHandler(key) => key.encode(),
			Self::Series(key) => key.encode(),
			Self::NamespaceSeries(key) => key.encode(),
			Self::SeriesMetadata(key) => key.encode(),
			Self::Identity(key) => key.encode(),
			Self::Role(key) => key.encode(),
			Self::GrantedRole(key) => key.encode(),
			Self::Policy(key) => key.encode(),
			Self::PolicyOp(key) => key.encode(),
			Self::Migration(key) => key.encode(),
			Self::MigrationEvent(key) => key.encode(),
			Self::Authentication(key) => key.encode(),
			Self::ConfigStorage(key) => key.encode(),
			Self::Token(key) => key.encode(),
			Self::Source(key) => key.encode(),
			Self::NamespaceSource(key) => key.encode(),
			Self::Sink(key) => key.encode(),
			Self::NamespaceSink(key) => key.encode(),
			Self::RowSettings(key) => key.encode(),
			Self::Procedure(key) => key.encode(),
			Self::NamespaceProcedure(key) => key.encode(),
			Self::ProcedureParam(key) => key.encode(),
			Self::Binding(key) => key.encode(),
			Self::NamespaceBinding(key) => key.encode(),
			Self::OperatorSettings(key) => key.encode(),
			Self::ColumnSnapshot(key) => key.encode(),
			Self::SeriesColumnSnapshot(key) => key.encode(),
			Self::TableColumnSnapshot(key) => key.encode(),
			Self::VersionEpoch(key) => key.encode(),
			Self::IdentityAttribute(key) => key.encode(),
			Self::IdentityAttributeValue(key) => key.encode(),
			Self::PartitionedRow(key) => key.encode(),
			Self::Partition(key) => key.encode(),
			Self::Queue(key) => key.encode(),
			Self::NamespaceQueue(key) => key.encode(),
			Self::QueueDeduplication(key) => key.encode(),
			Self::Relationship(key) => key.encode(),
			Self::SeriesRow(key) => key.encode(),
			Self::PartitionedSeriesRow(key) => key.encode(),
			Self::QueuePartition(key) => key.encode(),
			Self::QueueItemState(key) => key.encode(),
			Self::QueueDue(key) => key.encode(),
			Self::QueueAttempt(key) => key.encode(),
			Self::QueueKeyActive(key) => key.encode(),
			Self::SortedViewRow(key) => key.encode(),
			Self::PartitionedSortedViewRow(key) => key.encode(),
		}
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		match KeyTag::of(key)? {
			KeyTag::Namespace => NamespaceKey::decode(key).map(Self::Namespace),
			KeyTag::Table => TableKey::decode(key).map(Self::Table),
			KeyTag::Row => RowKey::decode(key).map(Self::Row),
			KeyTag::NamespaceTable => NamespaceTableKey::decode(key).map(Self::NamespaceTable),
			KeyTag::SystemSequence => SystemSequenceKey::decode(key).map(Self::SystemSequence),
			KeyTag::Columns => ColumnsKey::decode(key).map(Self::Columns),
			KeyTag::Column => ColumnKey::decode(key).map(Self::Column),
			KeyTag::RowSequence => RowSequenceKey::decode(key).map(Self::RowSequence),
			KeyTag::ColumnProperty => ColumnPropertyKey::decode(key).map(Self::ColumnProperty),
			KeyTag::SystemVersion => SystemVersionKey::decode(key).map(Self::SystemVersion),
			KeyTag::TransactionVersion => TransactionVersionKey::decode(key).map(Self::TransactionVersion),
			KeyTag::Index => IndexKey::decode(key).map(Self::Index),
			KeyTag::IndexEntry => IndexEntryKey::decode(key).map(Self::IndexEntry),
			KeyTag::ColumnSequence => ColumnSequenceKey::decode(key).map(Self::ColumnSequence),
			KeyTag::CdcConsumer => CdcConsumerKey::decode(key).map(Self::CdcConsumer),
			KeyTag::View => ViewKey::decode(key).map(Self::View),
			KeyTag::NamespaceView => NamespaceViewKey::decode(key).map(Self::NamespaceView),
			KeyTag::PrimaryKey => PrimaryKeyKey::decode(key).map(Self::PrimaryKey),
			KeyTag::OperatorState => OperatorStateKey::decode(key).map(Self::OperatorState),
			KeyTag::RingBuffer => RingBufferKey::decode(key).map(Self::RingBuffer),
			KeyTag::NamespaceRingBuffer => {
				NamespaceRingBufferKey::decode(key).map(Self::NamespaceRingBuffer)
			}
			KeyTag::RingBufferMetadata => RingBufferMetadataKey::decode(key).map(Self::RingBufferMetadata),
			KeyTag::Flow => FlowKey::decode(key).map(Self::Flow),
			KeyTag::NamespaceFlow => NamespaceFlowKey::decode(key).map(Self::NamespaceFlow),
			KeyTag::Operator => OperatorKey::decode(key).map(Self::Operator),
			KeyTag::OperatorByFlow => OperatorByFlowKey::decode(key).map(Self::OperatorByFlow),
			KeyTag::FlowEdge => FlowEdgeKey::decode(key).map(Self::FlowEdge),
			KeyTag::FlowEdgeByFlow => FlowEdgeByFlowKey::decode(key).map(Self::FlowEdgeByFlow),
			KeyTag::OutputFrontier => OutputFrontierKey::decode(key).map(Self::OutputFrontier),
			KeyTag::Dictionary => DictionaryKey::decode(key).map(Self::Dictionary),
			KeyTag::DictionaryEntry => DictionaryEntryKey::decode(key).map(Self::DictionaryEntry),
			KeyTag::DictionaryEntryIndex => {
				DictionaryEntryIndexKey::decode(key).map(Self::DictionaryEntryIndex)
			}
			KeyTag::NamespaceDictionary => {
				NamespaceDictionaryKey::decode(key).map(Self::NamespaceDictionary)
			}
			KeyTag::Metric => decode_metric(key),
			KeyTag::FlowVersion => FlowVersionKey::decode(key).map(Self::FlowVersion),
			KeyTag::RowShape => RowShapeKey::decode(key).map(Self::RowShape),
			KeyTag::RowShapeField => RowShapeFieldKey::decode(key).map(Self::RowShapeField),
			KeyTag::SumType => SumTypeKey::decode(key).map(Self::SumType),
			KeyTag::NamespaceSumType => NamespaceSumTypeKey::decode(key).map(Self::NamespaceSumType),
			KeyTag::Handler => HandlerKey::decode(key).map(Self::Handler),
			KeyTag::NamespaceHandler => NamespaceHandlerKey::decode(key).map(Self::NamespaceHandler),
			KeyTag::VariantHandler => VariantHandlerKey::decode(key).map(Self::VariantHandler),
			KeyTag::Series => SeriesKey::decode(key).map(Self::Series),
			KeyTag::NamespaceSeries => NamespaceSeriesKey::decode(key).map(Self::NamespaceSeries),
			KeyTag::SeriesMetadata => SeriesMetadataKey::decode(key).map(Self::SeriesMetadata),
			KeyTag::Identity => IdentityKey::decode(key).map(Self::Identity),
			KeyTag::Role => RoleKey::decode(key).map(Self::Role),
			KeyTag::GrantedRole => GrantedRoleKey::decode(key).map(Self::GrantedRole),
			KeyTag::Policy => PolicyKey::decode(key).map(Self::Policy),
			KeyTag::PolicyOp => PolicyOpKey::decode(key).map(Self::PolicyOp),
			KeyTag::Migration => MigrationKey::decode(key).map(Self::Migration),
			KeyTag::MigrationEvent => MigrationEventKey::decode(key).map(Self::MigrationEvent),
			KeyTag::Authentication => AuthenticationKey::decode(key).map(Self::Authentication),
			KeyTag::ConfigStorage => ConfigStorageKey::decode(key).map(Self::ConfigStorage),
			KeyTag::Token => TokenKey::decode(key).map(Self::Token),
			KeyTag::Source => SourceKey::decode(key).map(Self::Source),
			KeyTag::NamespaceSource => NamespaceSourceKey::decode(key).map(Self::NamespaceSource),
			KeyTag::Sink => SinkKey::decode(key).map(Self::Sink),
			KeyTag::NamespaceSink => NamespaceSinkKey::decode(key).map(Self::NamespaceSink),
			KeyTag::RowSettings => RowSettingsKey::decode(key).map(Self::RowSettings),
			KeyTag::Procedure => ProcedureKey::decode(key).map(Self::Procedure),
			KeyTag::NamespaceProcedure => NamespaceProcedureKey::decode(key).map(Self::NamespaceProcedure),
			KeyTag::ProcedureParam => ProcedureParamKey::decode(key).map(Self::ProcedureParam),
			KeyTag::Binding => BindingKey::decode(key).map(Self::Binding),
			KeyTag::NamespaceBinding => NamespaceBindingKey::decode(key).map(Self::NamespaceBinding),
			KeyTag::OperatorSettings => OperatorSettingsKey::decode(key).map(Self::OperatorSettings),
			KeyTag::ColumnSnapshot => ColumnSnapshotKey::decode(key).map(Self::ColumnSnapshot),
			KeyTag::SeriesColumnSnapshot => {
				SeriesColumnSnapshotKey::decode(key).map(Self::SeriesColumnSnapshot)
			}
			KeyTag::TableColumnSnapshot => {
				TableColumnSnapshotKey::decode(key).map(Self::TableColumnSnapshot)
			}
			KeyTag::VersionEpoch => VersionEpochKey::decode(key).map(Self::VersionEpoch),
			KeyTag::IdentityAttribute => IdentityAttributeKey::decode(key).map(Self::IdentityAttribute),
			KeyTag::IdentityAttributeValue => {
				IdentityAttributeValueKey::decode(key).map(Self::IdentityAttributeValue)
			}
			KeyTag::PartitionedRow => PartitionedRowKey::decode(key).map(Self::PartitionedRow),
			KeyTag::Partition => PartitionKey::decode(key).map(Self::Partition),
			KeyTag::Queue => QueueKey::decode(key).map(Self::Queue),
			KeyTag::NamespaceQueue => NamespaceQueueKey::decode(key).map(Self::NamespaceQueue),
			KeyTag::QueueDeduplication => QueueDeduplicationKey::decode(key).map(Self::QueueDeduplication),
			KeyTag::Relationship => RelationshipKey::decode(key).map(Self::Relationship),
			KeyTag::SeriesRow => SeriesRowKey::decode(key).map(Self::SeriesRow),
			KeyTag::PartitionedSeriesRow => {
				PartitionedSeriesRowKey::decode(key).map(Self::PartitionedSeriesRow)
			}
			KeyTag::QueuePartition => QueuePartitionKey::decode(key).map(Self::QueuePartition),
			KeyTag::QueueItemState => QueueItemStateKey::decode(key).map(Self::QueueItemState),
			KeyTag::QueueDue => QueueDueKey::decode(key).map(Self::QueueDue),
			KeyTag::QueueAttempt => QueueAttemptKey::decode(key).map(Self::QueueAttempt),
			KeyTag::QueueKeyActive => QueueKeyActiveKey::decode(key).map(Self::QueueKeyActive),
			KeyTag::SortedViewRow => SortedViewRowKey::decode(key).map(Self::SortedViewRow),
			KeyTag::PartitionedSortedViewRow => {
				PartitionedSortedViewRowKey::decode(key).map(Self::PartitionedSortedViewRow)
			}
		}
	}
}

fn decode_metric(key: &EncodedKey) -> Option<TaggedKey> {
	MetricStorageKey::decode(key)
		.map(MetricKey::Storage)
		.or_else(|| MetricCdcKey::decode(key).map(MetricKey::Cdc))
		.map(TaggedKey::Metric)
}

impl From<NamespaceKey> for TaggedKey {
	fn from(key: NamespaceKey) -> Self {
		Self::Namespace(key)
	}
}

impl From<TableKey> for TaggedKey {
	fn from(key: TableKey) -> Self {
		Self::Table(key)
	}
}

impl From<RowKey> for TaggedKey {
	fn from(key: RowKey) -> Self {
		Self::Row(key)
	}
}

impl From<NamespaceTableKey> for TaggedKey {
	fn from(key: NamespaceTableKey) -> Self {
		Self::NamespaceTable(key)
	}
}

impl From<SystemSequenceKey> for TaggedKey {
	fn from(key: SystemSequenceKey) -> Self {
		Self::SystemSequence(key)
	}
}

impl From<ColumnsKey> for TaggedKey {
	fn from(key: ColumnsKey) -> Self {
		Self::Columns(key)
	}
}

impl From<ColumnKey> for TaggedKey {
	fn from(key: ColumnKey) -> Self {
		Self::Column(key)
	}
}

impl From<RowSequenceKey> for TaggedKey {
	fn from(key: RowSequenceKey) -> Self {
		Self::RowSequence(key)
	}
}

impl From<ColumnPropertyKey> for TaggedKey {
	fn from(key: ColumnPropertyKey) -> Self {
		Self::ColumnProperty(key)
	}
}

impl From<SystemVersionKey> for TaggedKey {
	fn from(key: SystemVersionKey) -> Self {
		Self::SystemVersion(key)
	}
}

impl From<TransactionVersionKey> for TaggedKey {
	fn from(key: TransactionVersionKey) -> Self {
		Self::TransactionVersion(key)
	}
}

impl From<IndexKey> for TaggedKey {
	fn from(key: IndexKey) -> Self {
		Self::Index(key)
	}
}

impl From<IndexEntryKey> for TaggedKey {
	fn from(key: IndexEntryKey) -> Self {
		Self::IndexEntry(key)
	}
}

impl From<ColumnSequenceKey> for TaggedKey {
	fn from(key: ColumnSequenceKey) -> Self {
		Self::ColumnSequence(key)
	}
}

impl From<CdcConsumerKey> for TaggedKey {
	fn from(key: CdcConsumerKey) -> Self {
		Self::CdcConsumer(key)
	}
}

impl From<ViewKey> for TaggedKey {
	fn from(key: ViewKey) -> Self {
		Self::View(key)
	}
}

impl From<NamespaceViewKey> for TaggedKey {
	fn from(key: NamespaceViewKey) -> Self {
		Self::NamespaceView(key)
	}
}

impl From<PrimaryKeyKey> for TaggedKey {
	fn from(key: PrimaryKeyKey) -> Self {
		Self::PrimaryKey(key)
	}
}

impl From<OperatorStateKey> for TaggedKey {
	fn from(key: OperatorStateKey) -> Self {
		Self::OperatorState(key)
	}
}

impl From<RingBufferKey> for TaggedKey {
	fn from(key: RingBufferKey) -> Self {
		Self::RingBuffer(key)
	}
}

impl From<NamespaceRingBufferKey> for TaggedKey {
	fn from(key: NamespaceRingBufferKey) -> Self {
		Self::NamespaceRingBuffer(key)
	}
}

impl From<RingBufferMetadataKey> for TaggedKey {
	fn from(key: RingBufferMetadataKey) -> Self {
		Self::RingBufferMetadata(key)
	}
}

impl From<FlowKey> for TaggedKey {
	fn from(key: FlowKey) -> Self {
		Self::Flow(key)
	}
}

impl From<NamespaceFlowKey> for TaggedKey {
	fn from(key: NamespaceFlowKey) -> Self {
		Self::NamespaceFlow(key)
	}
}

impl From<OperatorKey> for TaggedKey {
	fn from(key: OperatorKey) -> Self {
		Self::Operator(key)
	}
}

impl From<OperatorByFlowKey> for TaggedKey {
	fn from(key: OperatorByFlowKey) -> Self {
		Self::OperatorByFlow(key)
	}
}

impl From<FlowEdgeKey> for TaggedKey {
	fn from(key: FlowEdgeKey) -> Self {
		Self::FlowEdge(key)
	}
}

impl From<FlowEdgeByFlowKey> for TaggedKey {
	fn from(key: FlowEdgeByFlowKey) -> Self {
		Self::FlowEdgeByFlow(key)
	}
}

impl From<OutputFrontierKey> for TaggedKey {
	fn from(key: OutputFrontierKey) -> Self {
		Self::OutputFrontier(key)
	}
}

impl From<DictionaryKey> for TaggedKey {
	fn from(key: DictionaryKey) -> Self {
		Self::Dictionary(key)
	}
}

impl From<DictionaryEntryKey> for TaggedKey {
	fn from(key: DictionaryEntryKey) -> Self {
		Self::DictionaryEntry(key)
	}
}

impl From<DictionaryEntryIndexKey> for TaggedKey {
	fn from(key: DictionaryEntryIndexKey) -> Self {
		Self::DictionaryEntryIndex(key)
	}
}

impl From<NamespaceDictionaryKey> for TaggedKey {
	fn from(key: NamespaceDictionaryKey) -> Self {
		Self::NamespaceDictionary(key)
	}
}

impl From<FlowVersionKey> for TaggedKey {
	fn from(key: FlowVersionKey) -> Self {
		Self::FlowVersion(key)
	}
}

impl From<RowShapeKey> for TaggedKey {
	fn from(key: RowShapeKey) -> Self {
		Self::RowShape(key)
	}
}

impl From<RowShapeFieldKey> for TaggedKey {
	fn from(key: RowShapeFieldKey) -> Self {
		Self::RowShapeField(key)
	}
}

impl From<SumTypeKey> for TaggedKey {
	fn from(key: SumTypeKey) -> Self {
		Self::SumType(key)
	}
}

impl From<NamespaceSumTypeKey> for TaggedKey {
	fn from(key: NamespaceSumTypeKey) -> Self {
		Self::NamespaceSumType(key)
	}
}

impl From<HandlerKey> for TaggedKey {
	fn from(key: HandlerKey) -> Self {
		Self::Handler(key)
	}
}

impl From<NamespaceHandlerKey> for TaggedKey {
	fn from(key: NamespaceHandlerKey) -> Self {
		Self::NamespaceHandler(key)
	}
}

impl From<VariantHandlerKey> for TaggedKey {
	fn from(key: VariantHandlerKey) -> Self {
		Self::VariantHandler(key)
	}
}

impl From<SeriesKey> for TaggedKey {
	fn from(key: SeriesKey) -> Self {
		Self::Series(key)
	}
}

impl From<NamespaceSeriesKey> for TaggedKey {
	fn from(key: NamespaceSeriesKey) -> Self {
		Self::NamespaceSeries(key)
	}
}

impl From<SeriesMetadataKey> for TaggedKey {
	fn from(key: SeriesMetadataKey) -> Self {
		Self::SeriesMetadata(key)
	}
}

impl From<IdentityKey> for TaggedKey {
	fn from(key: IdentityKey) -> Self {
		Self::Identity(key)
	}
}

impl From<RoleKey> for TaggedKey {
	fn from(key: RoleKey) -> Self {
		Self::Role(key)
	}
}

impl From<GrantedRoleKey> for TaggedKey {
	fn from(key: GrantedRoleKey) -> Self {
		Self::GrantedRole(key)
	}
}

impl From<PolicyKey> for TaggedKey {
	fn from(key: PolicyKey) -> Self {
		Self::Policy(key)
	}
}

impl From<PolicyOpKey> for TaggedKey {
	fn from(key: PolicyOpKey) -> Self {
		Self::PolicyOp(key)
	}
}

impl From<MigrationKey> for TaggedKey {
	fn from(key: MigrationKey) -> Self {
		Self::Migration(key)
	}
}

impl From<MigrationEventKey> for TaggedKey {
	fn from(key: MigrationEventKey) -> Self {
		Self::MigrationEvent(key)
	}
}

impl From<AuthenticationKey> for TaggedKey {
	fn from(key: AuthenticationKey) -> Self {
		Self::Authentication(key)
	}
}

impl From<ConfigStorageKey> for TaggedKey {
	fn from(key: ConfigStorageKey) -> Self {
		Self::ConfigStorage(key)
	}
}

impl From<TokenKey> for TaggedKey {
	fn from(key: TokenKey) -> Self {
		Self::Token(key)
	}
}

impl From<SourceKey> for TaggedKey {
	fn from(key: SourceKey) -> Self {
		Self::Source(key)
	}
}

impl From<NamespaceSourceKey> for TaggedKey {
	fn from(key: NamespaceSourceKey) -> Self {
		Self::NamespaceSource(key)
	}
}

impl From<SinkKey> for TaggedKey {
	fn from(key: SinkKey) -> Self {
		Self::Sink(key)
	}
}

impl From<NamespaceSinkKey> for TaggedKey {
	fn from(key: NamespaceSinkKey) -> Self {
		Self::NamespaceSink(key)
	}
}

impl From<RowSettingsKey> for TaggedKey {
	fn from(key: RowSettingsKey) -> Self {
		Self::RowSettings(key)
	}
}

impl From<ProcedureKey> for TaggedKey {
	fn from(key: ProcedureKey) -> Self {
		Self::Procedure(key)
	}
}

impl From<NamespaceProcedureKey> for TaggedKey {
	fn from(key: NamespaceProcedureKey) -> Self {
		Self::NamespaceProcedure(key)
	}
}

impl From<ProcedureParamKey> for TaggedKey {
	fn from(key: ProcedureParamKey) -> Self {
		Self::ProcedureParam(key)
	}
}

impl From<BindingKey> for TaggedKey {
	fn from(key: BindingKey) -> Self {
		Self::Binding(key)
	}
}

impl From<NamespaceBindingKey> for TaggedKey {
	fn from(key: NamespaceBindingKey) -> Self {
		Self::NamespaceBinding(key)
	}
}

impl From<OperatorSettingsKey> for TaggedKey {
	fn from(key: OperatorSettingsKey) -> Self {
		Self::OperatorSettings(key)
	}
}

impl From<ColumnSnapshotKey> for TaggedKey {
	fn from(key: ColumnSnapshotKey) -> Self {
		Self::ColumnSnapshot(key)
	}
}

impl From<SeriesColumnSnapshotKey> for TaggedKey {
	fn from(key: SeriesColumnSnapshotKey) -> Self {
		Self::SeriesColumnSnapshot(key)
	}
}

impl From<TableColumnSnapshotKey> for TaggedKey {
	fn from(key: TableColumnSnapshotKey) -> Self {
		Self::TableColumnSnapshot(key)
	}
}

impl From<VersionEpochKey> for TaggedKey {
	fn from(key: VersionEpochKey) -> Self {
		Self::VersionEpoch(key)
	}
}

impl From<IdentityAttributeKey> for TaggedKey {
	fn from(key: IdentityAttributeKey) -> Self {
		Self::IdentityAttribute(key)
	}
}

impl From<IdentityAttributeValueKey> for TaggedKey {
	fn from(key: IdentityAttributeValueKey) -> Self {
		Self::IdentityAttributeValue(key)
	}
}

impl From<PartitionedRowKey> for TaggedKey {
	fn from(key: PartitionedRowKey) -> Self {
		Self::PartitionedRow(key)
	}
}

impl From<PartitionKey> for TaggedKey {
	fn from(key: PartitionKey) -> Self {
		Self::Partition(key)
	}
}

impl From<QueueKey> for TaggedKey {
	fn from(key: QueueKey) -> Self {
		Self::Queue(key)
	}
}

impl From<NamespaceQueueKey> for TaggedKey {
	fn from(key: NamespaceQueueKey) -> Self {
		Self::NamespaceQueue(key)
	}
}

impl From<QueueDeduplicationKey> for TaggedKey {
	fn from(key: QueueDeduplicationKey) -> Self {
		Self::QueueDeduplication(key)
	}
}

impl From<RelationshipKey> for TaggedKey {
	fn from(key: RelationshipKey) -> Self {
		Self::Relationship(key)
	}
}

impl From<SeriesRowKey> for TaggedKey {
	fn from(key: SeriesRowKey) -> Self {
		Self::SeriesRow(key)
	}
}

impl From<PartitionedSeriesRowKey> for TaggedKey {
	fn from(key: PartitionedSeriesRowKey) -> Self {
		Self::PartitionedSeriesRow(key)
	}
}

impl From<QueuePartitionKey> for TaggedKey {
	fn from(key: QueuePartitionKey) -> Self {
		Self::QueuePartition(key)
	}
}

impl From<QueueItemStateKey> for TaggedKey {
	fn from(key: QueueItemStateKey) -> Self {
		Self::QueueItemState(key)
	}
}

impl From<QueueDueKey> for TaggedKey {
	fn from(key: QueueDueKey) -> Self {
		Self::QueueDue(key)
	}
}

impl From<QueueAttemptKey> for TaggedKey {
	fn from(key: QueueAttemptKey) -> Self {
		Self::QueueAttempt(key)
	}
}

impl From<QueueKeyActiveKey> for TaggedKey {
	fn from(key: QueueKeyActiveKey) -> Self {
		Self::QueueKeyActive(key)
	}
}

impl From<SortedViewRowKey> for TaggedKey {
	fn from(key: SortedViewRowKey) -> Self {
		Self::SortedViewRow(key)
	}
}

impl From<PartitionedSortedViewRowKey> for TaggedKey {
	fn from(key: PartitionedSortedViewRowKey) -> Self {
		Self::PartitionedSortedViewRow(key)
	}
}

impl From<MetricKey> for TaggedKey {
	fn from(key: MetricKey) -> Self {
		Self::Metric(key)
	}
}

impl From<MetricStorageKey> for TaggedKey {
	fn from(key: MetricStorageKey) -> Self {
		Self::Metric(MetricKey::Storage(key))
	}
}

impl From<MetricCdcKey> for TaggedKey {
	fn from(key: MetricCdcKey) -> Self {
		Self::Metric(MetricKey::Cdc(key))
	}
}

fn desc<T: Ord + ?Sized>(left: &T, right: &T) -> Ordering {
	right.cmp(left)
}

fn object_cmp(left: &ObjectId, right: &ObjectId) -> Ordering {
	left.type_tag().cmp(&right.type_tag()).then_with(|| right.as_u64().cmp(&left.as_u64()))
}

pub(crate) fn index_tag(index: &IndexId) -> u8 {
	match index {
		IndexId::Primary(_) => 0x01,
	}
}

fn tier_byte(tier: Tier) -> u8 {
	match tier {
		Tier::Buffer => 0x00,
		Tier::Persistent => 0x01,
	}
}

fn metrics_id_cmp(left: &MetricsId, right: &MetricsId) -> Ordering {
	match (left, right) {
		(MetricsId::System, MetricsId::System) => Ordering::Equal,
		(MetricsId::System, MetricsId::Object(_)) => Ordering::Less,
		(MetricsId::Object(_), MetricsId::System) => Ordering::Greater,
		(MetricsId::Object(left), MetricsId::Object(right)) => object_cmp(left, right),
	}
}

pub(crate) fn encode_values(values: &[Value]) -> EncodedKey {
	let mut serializer = KeySerializer::new();
	for value in values {
		serializer.extend_value(value);
	}
	serializer.to_encoded_key()
}

impl Ord for MetricKey {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Self::Cdc(left), Self::Cdc(right)) => metrics_id_cmp(&left.id, &right.id),
			(Self::Cdc(_), Self::Storage(_)) => Ordering::Less,
			(Self::Storage(_), Self::Cdc(_)) => Ordering::Greater,
			(Self::Storage(left), Self::Storage(right)) => {
				desc(&tier_byte(left.tier), &tier_byte(right.tier))
					.then_with(|| metrics_id_cmp(&left.id, &right.id))
			}
		}
	}
}

impl PartialOrd for MetricKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Eq for TaggedKey {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Width {
	U8,
	U16,
	U32,
	U64,
	U128,
	Varint,
}

impl Width {
	fn byte_len(self) -> usize {
		match self {
			Self::U8 => 1,
			Self::U16 => 2,
			Self::U32 => 4,
			Self::U64 => 8,
			Self::U128 => 16,
			Self::Varint => 0,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteEncoding {
	Fixed,
	Escaped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawEncoding {
	Verbatim,
	SortRun,
}

#[derive(Debug, Clone)]
pub enum Field<'a> {
	UAsc(Width, u128),
	UDesc(Width, u128),
	BytesDesc(ByteEncoding, Cow<'a, [u8]>),
	RawAsc(RawEncoding, Cow<'a, [u8]>),
}

impl Field<'_> {
	fn variant_rank(&self) -> u8 {
		match self {
			Self::UAsc(..) => 0,
			Self::UDesc(..) => 1,
			Self::BytesDesc(..) => 2,
			Self::RawAsc(..) => 3,
		}
	}

	pub fn is_truncation_of(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::RawAsc(RawEncoding::Verbatim, left), Self::RawAsc(RawEncoding::Verbatim, right)) => {
				left.len() < right.len() && right.starts_with(left)
			}
			_ => false,
		}
	}

	pub fn encode(&self, out: &mut Vec<u8>) {
		match self {
			Self::UAsc(width, value) => {
				debug_assert!(
					!matches!(width, Width::Varint),
					"there is no ascending varint primitive in the key codec"
				);
				out.extend_from_slice(&value.to_be_bytes()[16 - width.byte_len()..]);
			}
			Self::UDesc(Width::Varint, value) => encode_u128_varint(*value, out),
			Self::UDesc(width, value) => {
				out.extend_from_slice(&(!*value).to_be_bytes()[16 - width.byte_len()..]);
			}
			Self::BytesDesc(ByteEncoding::Fixed, bytes) => out.extend(bytes.iter().map(|byte| !byte)),
			Self::BytesDesc(ByteEncoding::Escaped, bytes) => encode_bytes(bytes, out),
			Self::RawAsc(RawEncoding::Verbatim, bytes) => out.extend_from_slice(bytes),
			Self::RawAsc(RawEncoding::SortRun, bytes) => encode_sort_run(bytes, out),
		}
	}
}

impl Ord for Field<'_> {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Self::UAsc(_, left), Self::UAsc(_, right)) => left.cmp(right),
			(Self::UDesc(_, left), Self::UDesc(_, right)) => right.cmp(left),
			(Self::BytesDesc(_, left), Self::BytesDesc(_, right)) => right.as_ref().cmp(left.as_ref()),
			(Self::RawAsc(_, left), Self::RawAsc(_, right)) => left.as_ref().cmp(right.as_ref()),
			(left, right) => left.variant_rank().cmp(&right.variant_rank()),
		}
	}
}

impl PartialOrd for Field<'_> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl PartialEq for Field<'_> {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Eq for Field<'_> {}

pub trait KeyFields {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]>;
}

impl KeyFields for MetricKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		match self {
			Self::Cdc(key) => key.fields(),
			Self::Storage(key) => key.fields(),
		}
	}
}

impl KeyFields for TaggedKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		match self {
			Self::Namespace(key) => key.fields(),
			Self::Table(key) => key.fields(),
			Self::Row(key) => key.fields(),
			Self::NamespaceTable(key) => key.fields(),
			Self::SystemSequence(key) => key.fields(),
			Self::Columns(key) => key.fields(),
			Self::Column(key) => key.fields(),
			Self::RowSequence(key) => key.fields(),
			Self::ColumnProperty(key) => key.fields(),
			Self::SystemVersion(key) => key.fields(),
			Self::TransactionVersion(key) => key.fields(),
			Self::Index(key) => key.fields(),
			Self::IndexEntry(key) => key.fields(),
			Self::ColumnSequence(key) => key.fields(),
			Self::CdcConsumer(key) => key.fields(),
			Self::View(key) => key.fields(),
			Self::NamespaceView(key) => key.fields(),
			Self::PrimaryKey(key) => key.fields(),
			Self::OperatorState(key) => key.fields(),
			Self::RingBuffer(key) => key.fields(),
			Self::NamespaceRingBuffer(key) => key.fields(),
			Self::RingBufferMetadata(key) => key.fields(),
			Self::Flow(key) => key.fields(),
			Self::NamespaceFlow(key) => key.fields(),
			Self::Operator(key) => key.fields(),
			Self::OperatorByFlow(key) => key.fields(),
			Self::FlowEdge(key) => key.fields(),
			Self::FlowEdgeByFlow(key) => key.fields(),
			Self::OutputFrontier(key) => key.fields(),
			Self::Dictionary(key) => key.fields(),
			Self::DictionaryEntry(key) => key.fields(),
			Self::DictionaryEntryIndex(key) => key.fields(),
			Self::NamespaceDictionary(key) => key.fields(),
			Self::Metric(key) => key.fields(),
			Self::FlowVersion(key) => key.fields(),
			Self::RowShape(key) => key.fields(),
			Self::RowShapeField(key) => key.fields(),
			Self::SumType(key) => key.fields(),
			Self::NamespaceSumType(key) => key.fields(),
			Self::Handler(key) => key.fields(),
			Self::NamespaceHandler(key) => key.fields(),
			Self::VariantHandler(key) => key.fields(),
			Self::Series(key) => key.fields(),
			Self::NamespaceSeries(key) => key.fields(),
			Self::SeriesMetadata(key) => key.fields(),
			Self::Identity(key) => key.fields(),
			Self::Role(key) => key.fields(),
			Self::GrantedRole(key) => key.fields(),
			Self::Policy(key) => key.fields(),
			Self::PolicyOp(key) => key.fields(),
			Self::Migration(key) => key.fields(),
			Self::MigrationEvent(key) => key.fields(),
			Self::Authentication(key) => key.fields(),
			Self::ConfigStorage(key) => key.fields(),
			Self::Token(key) => key.fields(),
			Self::Source(key) => key.fields(),
			Self::NamespaceSource(key) => key.fields(),
			Self::Sink(key) => key.fields(),
			Self::NamespaceSink(key) => key.fields(),
			Self::RowSettings(key) => key.fields(),
			Self::Procedure(key) => key.fields(),
			Self::NamespaceProcedure(key) => key.fields(),
			Self::ProcedureParam(key) => key.fields(),
			Self::Binding(key) => key.fields(),
			Self::NamespaceBinding(key) => key.fields(),
			Self::OperatorSettings(key) => key.fields(),
			Self::ColumnSnapshot(key) => key.fields(),
			Self::SeriesColumnSnapshot(key) => key.fields(),
			Self::TableColumnSnapshot(key) => key.fields(),
			Self::VersionEpoch(key) => key.fields(),
			Self::IdentityAttribute(key) => key.fields(),
			Self::IdentityAttributeValue(key) => key.fields(),
			Self::PartitionedRow(key) => key.fields(),
			Self::Partition(key) => key.fields(),
			Self::Queue(key) => key.fields(),
			Self::NamespaceQueue(key) => key.fields(),
			Self::QueueDeduplication(key) => key.fields(),
			Self::Relationship(key) => key.fields(),
			Self::SeriesRow(key) => key.fields(),
			Self::PartitionedSeriesRow(key) => key.fields(),
			Self::QueuePartition(key) => key.fields(),
			Self::QueueItemState(key) => key.fields(),
			Self::QueueDue(key) => key.fields(),
			Self::QueueAttempt(key) => key.fields(),
			Self::QueueKeyActive(key) => key.fields(),
			Self::SortedViewRow(key) => key.fields(),
			Self::PartitionedSortedViewRow(key) => key.fields(),
		}
	}
}

impl Ord for TaggedKey {
	fn cmp(&self, other: &Self) -> Ordering {
		desc(&(self.kind() as u8), &(other.kind() as u8)).then_with(|| self.fields().cmp(&other.fields()))
	}
}

impl PartialOrd for TaggedKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

#[cfg(test)]
mod tests {
	use std::{borrow::Cow, cmp::Ordering};

	use reifydb_codec::{key::encoded::EncodedKey, row::shape::fingerprint::RowShapeFingerprint};
	use reifydb_runtime::version_epoch::EpochSeconds;
	use reifydb_value::value::{
		Value, datetime::DateTime, dictionary::DictionaryId, identity::IdentityId, partition::Partition,
		row_number::RowNumber, sumtype::SumTypeId,
	};
	use smallvec::SmallVec;

	use super::{Field, KeyFields, MetricCdcKey, MetricKey, MetricStorageKey, RawEncoding, TaggedKey};
	use crate::{
		interface::{
			catalog::{
				config::ConfigKey,
				flow::{FlowEdgeId, FlowId, OperatorId},
				id::{
					BindingId, ColumnId, ColumnPropertyId, ColumnSnapshotId, HandlerId, IndexId,
					MigrationEventId, MigrationId, NamespaceId, PrimaryKeyId, ProcedureId, QueueId,
					RelationshipId, RingBufferId, SequenceId, SeriesId, SinkId, SourceId, TableId,
					ViewId,
				},
				metrics::MetricsId,
				object::ObjectId,
				storage::StorageId,
			},
			cdc::CdcConsumerId,
			store::Tier,
		},
		key::{
			bound::{OwnedField, TaggedKeyBound},
			catalog::{
				BindingKey, ColumnPropertyKey, DictionaryEntryIndexKey, DictionaryEntryKey,
				DictionaryKey, HandlerKey, IndexEntryKey, IndexKey, PrimaryKeyKey, RelationshipKey,
				SinkKey, SourceKey, SumTypeKey, TableKey, VariantHandlerKey, ViewKey,
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
			sort_run::SortRun,
			system::{
				MigrationEventKey, MigrationKey, SystemSequenceKey, SystemVersion, SystemVersionKey,
				TransactionVersionKey, VersionEpochKey,
			},
			tag::KeyTag,
		},
		value::index::encoded::EncodedIndexKey,
	};

	fn probe<K>(key: K) -> (TaggedKey, EncodedKey)
	where
		TaggedKey: From<K>,
	{
		let key = TaggedKey::from(key);
		let encoded = key.encode();
		(key, encoded)
	}

	fn probe_encodable<K>(key: K) -> (TaggedKey, EncodedKey)
	where
		TaggedKey: From<K>,
	{
		let key = TaggedKey::from(key);
		let encoded = key.encode();
		(key, encoded)
	}

	fn assert_ascending(probes: &[(TaggedKey, EncodedKey)]) {
		for pair in probes.windows(2) {
			let (left, left_bytes) = &pair[0];
			let (right, right_bytes) = &pair[1];
			assert_eq!(left.cmp(right), Ordering::Less, "{left:?} must sort before {right:?}");
			assert!(left_bytes < right_bytes, "{left:?} must encode before {right:?}");
		}
	}

	fn dictionary_entry(dictionary: u64, hash: u8) -> DictionaryEntryKey {
		DictionaryEntryKey {
			dictionary: DictionaryId(dictionary),
			hash: [hash; 16],
		}
	}

	fn series_row(variant_tag: Option<u8>) -> SeriesRowKey {
		SeriesRowKey {
			storage: StorageId::series(1),
			variant_tag,
			key: 0,
			sequence: 0,
		}
	}

	fn consumer(name: &str) -> CdcConsumerKey {
		CdcConsumerKey {
			consumer: CdcConsumerId::new(name),
		}
	}

	#[test]
	fn test_cross_kind_order_reverses_the_discriminant() {
		// Deriving Ord sorts by declaration index, which is the exact inverse of the encoded order
		// for every cross-kind pair; PendingWrites then merges backwards and raises no error.
		let mut probes = vec![
			probe(NamespaceKey {
				namespace: NamespaceId(1),
			}),
			probe(RowKey {
				storage: StorageId::table(1),
				row: RowNumber(1),
			}),
			probe_encodable(consumer("a")),
			probe(MetricCdcKey::new(MetricsId::System)),
			probe(MetricStorageKey::new(Tier::Buffer, MetricsId::System)),
			probe(SortedViewRowKey::new(StorageId::view(1), SortRun::new([0x10u8]), RowNumber(1))),
			probe(PartitionedSortedViewRowKey::new(
				StorageId::view(1),
				Partition(1),
				SortRun::new([0x10u8]),
				RowNumber(1),
			)),
		];
		probes.sort_by(|left, right| left.0.cmp(&right.0));

		let encoded: Vec<EncodedKey> = probes.iter().map(|(_, bytes)| bytes.clone()).collect();
		let mut expected = encoded.clone();
		expected.sort();
		assert_eq!(encoded, expected);

		assert_eq!(probes.first().unwrap().0.kind(), KeyTag::PartitionedSortedViewRow);
		assert_eq!(probes.last().unwrap().0.kind(), KeyTag::Namespace);
	}

	#[test]
	fn test_object_id_tag_ascends_while_its_id_descends() {
		// One field encodes in two directions: a raw tag then an inverted id. Reading it as a single
		// direction interleaves table columns with view columns.
		assert_ascending(&[
			probe(ColumnKey {
				object: ObjectId::table(2),
				column: ColumnId(1),
			}),
			probe(ColumnKey {
				object: ObjectId::table(1),
				column: ColumnId(1),
			}),
			probe(ColumnKey {
				object: ObjectId::view(2),
				column: ColumnId(1),
			}),
			probe(ColumnKey {
				object: ObjectId::view(1),
				column: ColumnId(1),
			}),
		]);
	}

	#[test]
	fn test_series_row_variant_tag_polarity() {
		// Option's own Ord puts None first and orders Some ascending; the encoder inverts both axes,
		// so delegating to it would place the untagged rows outside the scan that must find them.
		assert_ascending(&[probe(series_row(Some(2))), probe(series_row(Some(1))), probe(series_row(None))]);
	}

	#[test]
	fn test_partitioned_series_row_variant_tag_polarity() {
		let partitioned = |variant_tag| PartitionedSeriesRowKey {
			storage: StorageId::series(1),
			partition: Partition(1),
			variant_tag,
			key: 0,
			sequence: 0,
		};
		assert_ascending(&[probe(partitioned(Some(2))), probe(partitioned(Some(1))), probe(partitioned(None))]);
	}

	#[test]
	fn test_cdc_consumer_name_sorts_descending() {
		// CdcConsumerKey derives Ord over its String while encoding through extend_str, which
		// reverses; delegating to that derive returns the consumer list back to front.
		assert_ascending(&[
			probe_encodable(consumer("c")),
			probe_encodable(consumer("b")),
			probe_encodable(consumer("ab")),
			probe_encodable(consumer("a")),
		]);
	}

	#[test]
	fn test_config_key_sorts_by_its_rendered_name_not_its_discriminant() {
		// ConfigKey derives Ord over declaration order but encodes through its Display string.
		assert_ascending(&[
			probe_encodable(ConfigStorageKey::new(ConfigKey::QueryRowBatchSize)),
			probe_encodable(ConfigStorageKey::new(ConfigKey::OracleWindowSize)),
		]);
	}

	#[test]
	fn test_dictionary_entry_hash_sorts_ascending() {
		// The hash is the one field written raw, so inverting it with the rest loses every lookup.
		assert_ascending(&[
			probe(dictionary_entry(2, 0xFF)),
			probe(dictionary_entry(1, 0x00)),
			probe(dictionary_entry(1, 0x01)),
		]);
	}

	#[test]
	fn test_metric_cdc_sorts_before_metric_storage() {
		// Both families share one kind byte and are told apart by a sub-key written descending, so
		// 0x02 (cdc) has to sort ahead of 0x01 (storage).
		assert_ascending(&[
			probe(MetricCdcKey::new(MetricsId::System)),
			probe(MetricCdcKey::new(MetricsId::Object(ObjectId::table(2)))),
			probe(MetricCdcKey::new(MetricsId::Object(ObjectId::table(1)))),
			probe(MetricStorageKey::new(Tier::Persistent, MetricsId::System)),
			probe(MetricStorageKey::new(Tier::Buffer, MetricsId::System)),
			probe(MetricStorageKey::new(Tier::Buffer, MetricsId::Object(ObjectId::table(1)))),
		]);
	}

	#[test]
	fn test_index_and_index_entry_encode_the_index_id_differently() {
		// IndexKey writes a bare inverted id while IndexEntryKey writes a raw tag first, so one
		// comparison cannot serve both.
		assert_ascending(&[
			probe(IndexKey {
				object: ObjectId::table(1),
				index: IndexId::primary(PrimaryKeyId(2)),
			}),
			probe(IndexKey {
				object: ObjectId::table(1),
				index: IndexId::primary(PrimaryKeyId(1)),
			}),
		]);
		assert_ascending(&[
			probe_encodable(IndexEntryKey::new(
				ObjectId::table(1),
				IndexId::primary(PrimaryKeyId(1)),
				EncodedIndexKey::new([0x01u8]),
			)),
			probe_encodable(IndexEntryKey::new(
				ObjectId::table(1),
				IndexId::primary(PrimaryKeyId(1)),
				EncodedIndexKey::new([0x02u8]),
			)),
		]);
	}

	#[test]
	fn test_operator_state_suffix_sorts_ascending_under_descending_fields() {
		// The suffix is written raw so operator state scans stay in range order; inverting it would
		// hand the operator its own state in reverse.
		assert_ascending(&[
			probe_encodable(OperatorStateKey::new(OperatorId(1), GroupId::ROOT, KeyspaceId(2), [0x01u8])),
			probe_encodable(OperatorStateKey::new(OperatorId(1), GroupId::ROOT, KeyspaceId(2), [0x02u8])),
			probe_encodable(OperatorStateKey::new(OperatorId(1), GroupId::ROOT, KeyspaceId(1), [0x00u8])),
		]);
	}

	fn probes() -> Vec<(TaggedKey, EncodedKey)> {
		vec![
			probe(NamespaceKey {
				namespace: NamespaceId(1),
			}),
			probe(NamespaceKey {
				namespace: NamespaceId(2),
			}),
			probe(RowKey {
				storage: StorageId::table(1),
				row: RowNumber(1),
			}),
			probe(RowKey {
				storage: StorageId::table(1),
				row: RowNumber(2),
			}),
			probe(RowKey {
				storage: StorageId::view(1),
				row: RowNumber(1),
			}),
			probe(ColumnKey {
				object: ObjectId::table(1),
				column: ColumnId(1),
			}),
			probe(ColumnKey {
				object: ObjectId::queue(1),
				column: ColumnId(1),
			}),
			probe(IndexKey {
				object: ObjectId::table(1),
				index: IndexId::primary(PrimaryKeyId(1)),
			}),
			probe_encodable(IndexEntryKey::new(
				ObjectId::table(1),
				IndexId::primary(PrimaryKeyId(1)),
				EncodedIndexKey::new([0x01u8]),
			)),
			probe_encodable(IndexEntryKey::new(
				ObjectId::table(1),
				IndexId::primary(PrimaryKeyId(1)),
				EncodedIndexKey::new([0x01u8, 0x02]),
			)),
			probe_encodable(consumer("a")),
			probe_encodable(consumer("ab")),
			probe_encodable(consumer("b")),
			probe(dictionary_entry(1, 0x00)),
			probe(dictionary_entry(1, 0xFF)),
			probe(dictionary_entry(2, 0x00)),
			probe_encodable(DictionaryEntryIndexKey::new(DictionaryId(1), 1)),
			probe_encodable(DictionaryEntryIndexKey::new(DictionaryId(1), 1 << 20)),
			probe_encodable(DictionaryEntryIndexKey::new(DictionaryId(1), u128::from(u64::MAX) + 5)),
			probe_encodable(OperatorStateKey::new(OperatorId(1), GroupId::ROOT, KeyspaceId(1), [0x00u8])),
			probe_encodable(OperatorStateKey::new(OperatorId(1), GroupId::MAX, KeyspaceId(1), [0x00u8])),
			probe_encodable(ConfigStorageKey::new(ConfigKey::OracleWindowSize)),
			probe_encodable(ConfigStorageKey::new(ConfigKey::QueryRowBatchSize)),
			probe(series_row(Some(0))),
			probe(series_row(Some(7))),
			probe(series_row(None)),
			probe(QueueDueKey {
				queue: QueueId(1),
				partition: 0,
				due: DateTime::from_bits(1),
				row: RowNumber(1),
			}),
			probe(QueueDueKey {
				queue: QueueId(1),
				partition: 0,
				due: DateTime::from_bits(2),
				row: RowNumber(1),
			}),
			probe_encodable(QueueDeduplicationKey::new(QueueId(1), [0x01u8])),
			probe_encodable(QueueDeduplicationKey::new(QueueId(1), [0x01u8, 0x02])),
			probe_encodable(RingBufferMetadataKey::new(StorageId::ringbuffer(1))),
			probe_encodable(RingBufferMetadataKey {
				storage: StorageId::ringbuffer(1),
				partition_values: vec![Value::Uint8(1)],
			}),
			probe_encodable(RingBufferMetadataKey {
				storage: StorageId::ringbuffer(1),
				partition_values: vec![Value::Uint8(2)],
			}),
			probe(SortedViewRowKey::new(StorageId::view(1), SortRun::new([0x10u8]), RowNumber(1))),
			probe(SortedViewRowKey::new(StorageId::view(1), SortRun::new([0x10u8]), RowNumber(2))),
			probe(SortedViewRowKey::new(StorageId::view(1), SortRun::new([0x20u8]), RowNumber(1))),
			probe(PartitionedSortedViewRowKey::new(
				StorageId::view(1),
				Partition(1),
				SortRun::new([0x10u8]),
				RowNumber(1),
			)),
			probe(PartitionedSortedViewRowKey::new(
				StorageId::view(1),
				Partition(2),
				SortRun::new([0x10u8]),
				RowNumber(1),
			)),
			probe(MetricCdcKey::new(MetricsId::System)),
			probe(MetricCdcKey::new(MetricsId::Object(ObjectId::table(1)))),
			probe(MetricStorageKey::new(Tier::Buffer, MetricsId::System)),
			probe(MetricStorageKey::new(Tier::Persistent, MetricsId::System)),
			probe(SystemVersionKey {
				version: SystemVersion::Storage,
			}),
			probe(TransactionVersionKey {}),
			probe(TableKey {
				table: TableId(1),
			}),
		]
	}

	#[test]
	fn test_ord_agrees_with_encoded_order_for_every_pair() {
		// The whole point of the impl: byte order is the merge order, so any disagreement is a
		// silently wrong read rather than an error.
		let probes = probes();
		for (left, left_bytes) in &probes {
			for (right, right_bytes) in &probes {
				assert_eq!(left.cmp(right), left_bytes.cmp(right_bytes), "{left:?} vs {right:?}");
			}
		}
	}
	fn assert_projection_matches_bytes(samples: Vec<(TaggedKey, EncodedKey)>, label: &str) {
		assert!(samples.len() >= 2, "{label} needs at least two samples to exercise a direction");
		for (left, left_bytes) in &samples {
			for (right, right_bytes) in &samples {
				let projected = (right.kind() as u8)
					.cmp(&(left.kind() as u8))
					.then_with(|| left.fields().cmp(&right.fields()));
				assert_eq!(
					projected,
					left_bytes.cmp(right_bytes),
					"{label}: kind and fields() disagree with encoded byte order\n  a = \
					 {left:?}\n  b = {right:?}\n  a bytes = {:?}\n  b bytes = {:?}",
					left_bytes.as_slice(),
					right_bytes.as_slice()
				);
			}
		}
	}

	fn encode_via_fields(key: &TaggedKey) -> EncodedKey {
		let mut out = vec![!(key.kind() as u8)];
		for field in key.fields().iter() {
			field.encode(&mut out);
		}
		EncodedKey::new(out)
	}

	#[test]
	fn every_projection_replays_the_exact_bytes_its_encoder_wrote() {
		// fields() carries the ordering; the encoding tag on each Field carries the codec
		// primitive it came from. Ordering tests cannot see a wrong tag, because Fixed and
		// Escaped sort alike and every integer width compares the same way, so only replaying
		// the bytes catches one.
		let probes = all_probes();
		assert!(probes.len() >= 80, "the byte replay is only worth running over every key type");
		for (key, encoded) in probes {
			assert_eq!(encode_via_fields(&key), encoded, "{key:?}");
		}
	}

	#[test]
	fn every_hand_written_key_projects_to_the_order_its_encoder_produces() {
		// the derive emits a conformance test per key type; these seventeen write encode() by hand
		// and would otherwise have no sample varying each field, so a wrong direction would pass.
		assert_projection_matches_bytes(
			vec![
				probe(BindingKey {
					binding: BindingId(1),
				}),
				probe(BindingKey {
					binding: BindingId(2),
				}),
			],
			"BindingKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(ProcedureKey {
					procedure: ProcedureId::persistent(1),
				}),
				probe(ProcedureKey {
					procedure: ProcedureId::persistent(2),
				}),
			],
			"ProcedureKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(ProcedureParamKey {
					procedure: ProcedureId::persistent(1),
					param_index: 1,
				}),
				probe(ProcedureParamKey {
					procedure: ProcedureId::persistent(1),
					param_index: 2,
				}),
				probe(ProcedureParamKey {
					procedure: ProcedureId::persistent(2),
					param_index: 1,
				}),
			],
			"ProcedureParamKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(SinkKey {
					sink: SinkId(1),
				}),
				probe(SinkKey {
					sink: SinkId(2),
				}),
			],
			"SinkKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(SourceKey {
					source: SourceId(1),
				}),
				probe(SourceKey {
					source: SourceId(2),
				}),
			],
			"SourceKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(ViewKey {
					view: ViewId(1),
				}),
				probe(ViewKey {
					view: ViewId(2),
				}),
			],
			"ViewKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(SumTypeKey {
					sumtype: SumTypeId(1),
				}),
				probe(SumTypeKey {
					sumtype: SumTypeId(2),
				}),
			],
			"SumTypeKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe_encodable(DictionaryEntryIndexKey {
					dictionary: DictionaryId(1),
					id: 1,
				}),
				probe_encodable(DictionaryEntryIndexKey {
					dictionary: DictionaryId(1),
					id: u128::from(u64::MAX) + 1,
				}),
				probe_encodable(DictionaryEntryIndexKey {
					dictionary: DictionaryId(2),
					id: 1,
				}),
			],
			"DictionaryEntryIndexKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe_encodable(IndexEntryKey::new(
					ObjectId::table(1),
					IndexId::primary(PrimaryKeyId(1)),
					EncodedIndexKey::new([0x01u8]),
				)),
				probe_encodable(IndexEntryKey::new(
					ObjectId::table(1),
					IndexId::primary(PrimaryKeyId(1)),
					EncodedIndexKey::new([0x02u8]),
				)),
				probe_encodable(IndexEntryKey::new(
					ObjectId::table(1),
					IndexId::primary(PrimaryKeyId(2)),
					EncodedIndexKey::new([0x01u8]),
				)),
				probe_encodable(IndexEntryKey::new(
					ObjectId::view(1),
					IndexId::primary(PrimaryKeyId(1)),
					EncodedIndexKey::new([0x01u8]),
				)),
			],
			"IndexEntryKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe_encodable(CdcConsumerKey::new(CdcConsumerId::new("a"))),
				probe_encodable(CdcConsumerKey::new(CdcConsumerId::new("b"))),
				probe_encodable(CdcConsumerKey::new(CdcConsumerId::new("ab"))),
			],
			"CdcConsumerKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe_encodable(ConfigStorageKey::new(ConfigKey::QueryRowBatchSize)),
				probe_encodable(ConfigStorageKey::new(ConfigKey::OracleWindowSize)),
			],
			"ConfigStorageKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe_encodable(QueueDeduplicationKey::new(QueueId(1), [0x01u8])),
				probe_encodable(QueueDeduplicationKey::new(QueueId(1), [0x02u8])),
				probe_encodable(QueueDeduplicationKey::new(QueueId(1), [0x01u8, 0x00])),
				probe_encodable(QueueDeduplicationKey::new(QueueId(2), [0x01u8])),
			],
			"QueueDeduplicationKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe_encodable(OperatorStateKey::new(
					OperatorId(1),
					GroupId::ROOT,
					KeyspaceId(1),
					[0x01u8],
				)),
				probe_encodable(OperatorStateKey::new(
					OperatorId(1),
					GroupId::ROOT,
					KeyspaceId(1),
					[0x02u8],
				)),
				probe_encodable(OperatorStateKey::new(
					OperatorId(1),
					GroupId::ROOT,
					KeyspaceId(2),
					[0x01u8],
				)),
				probe_encodable(OperatorStateKey::new(
					OperatorId(1),
					GroupId::from_bytes([0x01u8; 24]),
					KeyspaceId(1),
					[0x01u8],
				)),
				probe_encodable(OperatorStateKey::new(
					OperatorId(2),
					GroupId::ROOT,
					KeyspaceId(1),
					[0x01u8],
				)),
			],
			"OperatorStateKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe_encodable(RingBufferMetadataKey {
					storage: StorageId::ringbuffer(1),
					partition_values: vec![Value::Uint8(1)],
				}),
				probe_encodable(RingBufferMetadataKey {
					storage: StorageId::ringbuffer(1),
					partition_values: vec![Value::Uint8(2)],
				}),
				probe_encodable(RingBufferMetadataKey {
					storage: StorageId::ringbuffer(2),
					partition_values: vec![Value::Uint8(1)],
				}),
			],
			"RingBufferMetadataKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(SortedViewRowKey::new(StorageId::view(1), SortRun::new([0x10u8]), RowNumber(1))),
				probe(SortedViewRowKey::new(StorageId::view(1), SortRun::new([0x10u8]), RowNumber(2))),
				probe(SortedViewRowKey::new(StorageId::view(1), SortRun::new([0x20u8]), RowNumber(1))),
				probe(SortedViewRowKey::new(
					StorageId::view(1),
					SortRun::new([0x10u8, 0x00]),
					RowNumber(1),
				)),
				probe(SortedViewRowKey::new(StorageId::view(2), SortRun::new([0x10u8]), RowNumber(1))),
			],
			"SortedViewRowKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(PartitionedSortedViewRowKey::new(
					StorageId::view(1),
					Partition(1),
					SortRun::new([0x10u8]),
					RowNumber(1),
				)),
				probe(PartitionedSortedViewRowKey::new(
					StorageId::view(1),
					Partition(1),
					SortRun::new([0x10u8]),
					RowNumber(2),
				)),
				probe(PartitionedSortedViewRowKey::new(
					StorageId::view(1),
					Partition(1),
					SortRun::new([0x20u8]),
					RowNumber(1),
				)),
				probe(PartitionedSortedViewRowKey::new(
					StorageId::view(1),
					Partition(2),
					SortRun::new([0x10u8]),
					RowNumber(1),
				)),
				probe(PartitionedSortedViewRowKey::new(
					StorageId::view(2),
					Partition(1),
					SortRun::new([0x10u8]),
					RowNumber(1),
				)),
			],
			"PartitionedSortedViewRowKey",
		);
		assert_projection_matches_bytes(
			vec![
				probe(MetricStorageKey::new(Tier::Buffer, MetricsId::System)),
				probe(MetricStorageKey::new(Tier::Persistent, MetricsId::System)),
				probe(MetricStorageKey::new(Tier::Buffer, MetricsId::Object(ObjectId::table(1)))),
				probe(MetricStorageKey::new(Tier::Buffer, MetricsId::Object(ObjectId::table(2)))),
				probe(MetricStorageKey::new(Tier::Buffer, MetricsId::Object(ObjectId::view(1)))),
				probe(MetricCdcKey::new(MetricsId::System)),
				probe(MetricCdcKey::new(MetricsId::Object(ObjectId::table(1)))),
				probe(MetricCdcKey::new(MetricsId::Object(ObjectId::table(2)))),
			],
			"MetricKey",
		);
	}

	fn all_probes() -> Vec<(TaggedKey, EncodedKey)> {
		vec![
			probe(NamespaceKey {
				namespace: NamespaceId(1),
			}),
			probe(TableKey {
				table: TableId(2),
			}),
			probe(RowKey {
				storage: StorageId::table(3),
				row: RowNumber(4),
			}),
			probe(NamespaceTableKey {
				namespace: NamespaceId(5),
				table: TableId(6),
			}),
			probe(SystemSequenceKey {
				sequence: SequenceId(7),
			}),
			probe(ColumnsKey {
				column: ColumnId(8),
			}),
			probe(ColumnKey {
				object: ObjectId::view(9),
				column: ColumnId(10),
			}),
			probe(RowSequenceKey {
				storage: StorageId::view(11),
			}),
			probe(ColumnPropertyKey {
				column: ColumnId(12),
				property: ColumnPropertyId(13),
			}),
			probe(SystemVersionKey {
				version: SystemVersion::Storage,
			}),
			probe(TransactionVersionKey {}),
			probe(IndexKey {
				object: ObjectId::table(14),
				index: IndexId::primary(PrimaryKeyId(15)),
			}),
			probe_encodable(IndexEntryKey::new(
				ObjectId::table(16),
				IndexId::primary(PrimaryKeyId(17)),
				EncodedIndexKey::new([0x01u8, 0x02]),
			)),
			probe(ColumnSequenceKey {
				object: ObjectId::table(18),
				column: ColumnId(19),
			}),
			probe_encodable(CdcConsumerKey {
				consumer: CdcConsumerId::new("consumer"),
			}),
			probe(ViewKey {
				view: ViewId(20),
			}),
			probe(NamespaceViewKey {
				namespace: NamespaceId(21),
				view: ViewId(22),
			}),
			probe(PrimaryKeyKey {
				primary_key: PrimaryKeyId(23),
			}),
			probe_encodable(OperatorStateKey::new(
				OperatorId(24),
				GroupId::ROOT,
				KeyspaceId(2),
				[0x01u8, 0x02],
			)),
			probe(RingBufferKey {
				ringbuffer: RingBufferId(25),
			}),
			probe(NamespaceRingBufferKey {
				namespace: NamespaceId(26),
				ringbuffer: RingBufferId(27),
			}),
			probe_encodable(RingBufferMetadataKey {
				storage: StorageId::ringbuffer(28),
				partition_values: vec![Value::Utf8("east".to_string()), Value::Uint8(3)],
			}),
			probe(FlowKey {
				flow: FlowId(29),
			}),
			probe(NamespaceFlowKey {
				namespace: NamespaceId(30),
				flow: FlowId(31),
			}),
			probe(OperatorKey {
				operator: OperatorId(32),
			}),
			probe(OperatorByFlowKey {
				flow: FlowId(33),
				operator: OperatorId(34),
			}),
			probe(FlowEdgeKey {
				edge: FlowEdgeId(35),
			}),
			probe(FlowEdgeByFlowKey {
				flow: FlowId(36),
				edge: FlowEdgeId(37),
			}),
			probe(OutputFrontierKey {
				object: ObjectId::view(38),
			}),
			probe(DictionaryKey {
				dictionary: DictionaryId(39),
			}),
			probe(DictionaryEntryKey {
				dictionary: DictionaryId(40),
				hash: [0x5Au8; 16],
			}),
			probe_encodable(DictionaryEntryIndexKey::new(DictionaryId(41), u128::from(u64::MAX) + 9)),
			probe(NamespaceDictionaryKey {
				namespace: NamespaceId(42),
				dictionary: DictionaryId(43),
			}),
			probe(MetricStorageKey::new(Tier::Persistent, MetricsId::Object(ObjectId::table(44)))),
			probe(MetricCdcKey::new(MetricsId::System)),
			probe(FlowVersionKey {
				flow: FlowId(45),
			}),
			probe(RowShapeKey {
				fingerprint: RowShapeFingerprint::new(46),
			}),
			probe(RowShapeFieldKey {
				shape_fingerprint: RowShapeFingerprint::new(47),
				field_index: 48,
			}),
			probe(SumTypeKey {
				sumtype: SumTypeId(49),
			}),
			probe(NamespaceSumTypeKey {
				namespace: NamespaceId(50),
				sumtype: SumTypeId(51),
			}),
			probe(HandlerKey {
				handler: HandlerId(52),
			}),
			probe(NamespaceHandlerKey {
				namespace: NamespaceId(53),
				handler: HandlerId(54),
			}),
			probe(VariantHandlerKey {
				namespace: NamespaceId(55),
				sumtype: SumTypeId(56),
				variant_tag: 57,
				handler: HandlerId(58),
			}),
			probe(SeriesKey {
				series: SeriesId(59),
			}),
			probe(NamespaceSeriesKey {
				namespace: NamespaceId(60),
				series: SeriesId(61),
			}),
			probe(SeriesMetadataKey {
				storage: StorageId::series(62),
			}),
			probe(IdentityKey {
				identity: IdentityId::anonymous(),
			}),
			probe(RoleKey {
				role: 63,
			}),
			probe(GrantedRoleKey {
				identity: IdentityId::root(),
				role: 64,
			}),
			probe(PolicyKey {
				policy: 65,
			}),
			probe(PolicyOpKey {
				policy: 66,
				op_index: 67,
			}),
			probe(MigrationKey {
				migration: MigrationId(68),
			}),
			probe(MigrationEventKey {
				event: MigrationEventId(69),
			}),
			probe(AuthenticationKey {
				authentication: 70,
			}),
			probe_encodable(ConfigStorageKey::new(ConfigKey::QueryRowBatchSize)),
			probe(TokenKey {
				token: 71,
			}),
			probe(SourceKey {
				source: SourceId(72),
			}),
			probe(NamespaceSourceKey {
				namespace: NamespaceId(73),
				source: SourceId(74),
			}),
			probe(SinkKey {
				sink: SinkId(75),
			}),
			probe(NamespaceSinkKey {
				namespace: NamespaceId(76),
				sink: SinkId(77),
			}),
			probe(RowSettingsKey {
				storage: StorageId::ringbuffer(78),
			}),
			probe(ProcedureKey {
				procedure: ProcedureId::persistent(79),
			}),
			probe(NamespaceProcedureKey {
				namespace: NamespaceId(80),
				procedure: ProcedureId::persistent(81),
			}),
			probe(ProcedureParamKey {
				procedure: ProcedureId::persistent(82),
				param_index: 83,
			}),
			probe(BindingKey {
				binding: BindingId(84),
			}),
			probe(NamespaceBindingKey {
				namespace: NamespaceId(85),
				binding: BindingId(86),
			}),
			probe(OperatorSettingsKey {
				operator: OperatorId(87),
			}),
			probe(ColumnSnapshotKey {
				snapshot: ColumnSnapshotId(88),
			}),
			probe(SeriesColumnSnapshotKey {
				series: SeriesId(89),
				snapshot: ColumnSnapshotId(90),
			}),
			probe(TableColumnSnapshotKey {
				table: TableId(91),
				snapshot: ColumnSnapshotId(92),
			}),
			probe(VersionEpochKey {
				bucket: EpochSeconds::new(93),
			}),
			probe(IdentityAttributeKey {
				attribute: 94,
			}),
			probe(IdentityAttributeValueKey {
				identity: IdentityId::anonymous(),
				attribute: 95,
			}),
			probe(PartitionedRowKey {
				storage: StorageId::table(96),
				partition: Partition(97),
				row: RowNumber(98),
			}),
			probe(PartitionKey {
				object: ObjectId::table(99),
				partition: Partition(100),
			}),
			probe(QueueKey {
				queue: QueueId(101),
			}),
			probe(NamespaceQueueKey {
				namespace: NamespaceId(102),
				queue: QueueId(103),
			}),
			probe_encodable(QueueDeduplicationKey::new(QueueId(104), [0xFFu8, 0x00, 0xFF])),
			probe(RelationshipKey {
				relationship: RelationshipId(105),
			}),
			probe(SeriesRowKey {
				storage: StorageId::series(106),
				variant_tag: Some(5),
				key: 107,
				sequence: 108,
			}),
			probe(PartitionedSeriesRowKey {
				storage: StorageId::series(109),
				partition: Partition(110),
				variant_tag: None,
				key: 111,
				sequence: 112,
			}),
			probe(QueuePartitionKey {
				queue: QueueId(113),
				partition: 114,
			}),
			probe(QueueItemStateKey {
				queue: QueueId(115),
				partition: 116,
				row: RowNumber(117),
			}),
			probe(QueueDueKey {
				queue: QueueId(118),
				partition: 119,
				due: DateTime::from_bits(120),
				row: RowNumber(121),
			}),
			probe(QueueAttemptKey {
				queue: QueueId(122),
				row: RowNumber(123),
				attempt: 124,
			}),
			probe(QueueKeyActiveKey {
				queue: QueueId(125),
				partition: 126,
				key_hash: 127,
				row: RowNumber(128),
			}),
			probe(SortedViewRowKey::new(
				StorageId::view(129),
				SortRun::new([0x00u8, 0xFF, 0x10]),
				RowNumber(130),
			)),
			probe(PartitionedSortedViewRowKey::new(
				StorageId::view(131),
				Partition(132),
				SortRun::new([0x10u8]),
				RowNumber(133),
			)),
		]
	}

	fn owned_field(field: &Field<'_>) -> OwnedField {
		match field {
			Field::UAsc(width, value) => Field::UAsc(*width, *value),
			Field::UDesc(width, value) => Field::UDesc(*width, *value),
			Field::BytesDesc(encoding, bytes) => {
				Field::BytesDesc(*encoding, Cow::Owned(bytes.as_ref().to_vec()))
			}
			Field::RawAsc(encoding, bytes) => Field::RawAsc(*encoding, Cow::Owned(bytes.as_ref().to_vec())),
		}
	}

	/// Every bound shape a producer can build for `key`: the bare kind and its successor, and each
	/// head of the key's own field list as both a `Prefix` and a `PrefixEnd`.
	///
	/// The last shape drops a byte from a trailing verbatim field, which is what
	/// `IndexEntryKey::key_prefix_range` does: a head whose final field is a truncation rather
	/// than a whole value. Terminated and fixed-width encodings are not truncated here because no
	/// producer truncates them, and a partial value under those encodings is not a byte prefix of
	/// the whole one.
	fn bounds_derived_from(key: &TaggedKey) -> Vec<TaggedKeyBound> {
		let mut out = vec![TaggedKeyBound::Kind(key.kind()), TaggedKeyBound::KindEnd(key.kind())];
		let fields = key.fields();

		for len in 0..=fields.len() {
			let head: SmallVec<[OwnedField; 6]> = fields[..len].iter().map(owned_field).collect();
			out.push(TaggedKeyBound::Prefix(key.kind(), head.clone()));
			if len > 0 {
				// `PrefixEnd` with no fields is not a shape a producer can reach: every
				// `TaggedKeyBoundRange::prefix` call passes fields, and the end of a whole
				// kind span is spelled `KindEnd`, which is generated above. The two
				// encode alike but order differently, since only `KindEnd` carries the
				// decrement in its kind byte rather than in the encoded increment.
				out.push(TaggedKeyBound::PrefixEnd(key.kind(), head));
			}
		}

		if let Some(Field::RawAsc(RawEncoding::Verbatim, bytes)) = fields.last() {
			if bytes.len() > 1 {
				let mut head: SmallVec<[OwnedField; 6]> =
					fields[..fields.len() - 1].iter().map(owned_field).collect();
				head.push(Field::RawAsc(
					RawEncoding::Verbatim,
					Cow::Owned(bytes[..bytes.len() - 1].to_vec()),
				));
				out.push(TaggedKeyBound::Prefix(key.kind(), head.clone()));
				out.push(TaggedKeyBound::PrefixEnd(key.kind(), head));
			}
		}

		out
	}

	#[test]
	fn every_bound_shape_orders_against_every_key_the_way_their_bytes_do() {
		// A bound exists only to delimit a scan, and the scan is merged on bytes. Where a bound
		// and a key disagree, a range silently admits or drops that row: the typed side of the
		// scan says one thing and the storage engine another, with no error either way.
		//
		// The shapes here are derived from the keys themselves rather than listed per producer,
		// so a new key type is covered the moment it joins the probe set, and every field
		// position is exercised rather than only the ones a producer happens to slice at today.
		let probes = all_probes();
		let bounds: Vec<TaggedKeyBound> = probes.iter().flat_map(|(key, _)| bounds_derived_from(key)).collect();

		for bound in &bounds {
			let bound_bytes = bound.encode();
			if bound_bytes.as_slice().is_empty() {
				// `PrefixEnd` collapses to nothing when every byte it would increment is
				// already 0xff. That is the encoding of an unbounded edge, which no
				// producer emits and `TaggedKeyBoundRange::empty` relies on.
				continue;
			}
			for (key, key_bytes) in &probes {
				let probe = TaggedKeyBound::Key(key.clone());
				assert_eq!(
					bound.cmp(&probe),
					bound_bytes.as_slice().cmp(key_bytes.as_slice()),
					"bound order disagrees with encoded order\n  bound = {bound:?}\n  key \
					 = {key:?}\n  bound bytes = {:02x?}\n  key bytes = {:02x?}",
					bound_bytes.as_slice(),
					key_bytes.as_slice()
				);
			}
		}
	}

	#[test]
	fn kind_end_is_the_spelling_that_agrees_with_bytes_for_a_whole_kind_span() {
		// `KindEnd(k)` and `PrefixEnd(k, [])` encode to the same byte, the start of the next kind
		// down, but only `KindEnd` also orders there. Producers must use `KindEnd`, which
		// `TaggedKeyBoundRange::kind` does; this pins that the correct spelling stays correct.
		for (key, key_bytes) in all_probes() {
			let end = TaggedKeyBound::KindEnd(key.kind());
			let end_bytes = end.encode();
			let probe = TaggedKeyBound::Key(key.clone());

			assert_eq!(
				end.cmp(&probe),
				end_bytes.as_slice().cmp(key_bytes.as_slice()),
				"KindEnd disagrees with its bytes for {key:?}"
			);
			assert_eq!(
				end_bytes.as_slice(),
				TaggedKeyBound::PrefixEnd(key.kind(), SmallVec::new()).encode().as_slice(),
				"KindEnd and an empty PrefixEnd must still encode alike for {key:?}"
			);
		}
	}

	#[test]
	fn test_every_variant_round_trips_through_the_shared_decoder() {
		// A key that comes back as the wrong variant rewrites the wrong row once Delta carries it.
		for (key, bytes) in all_probes() {
			assert_eq!(TaggedKey::decode(&bytes), Some(key.clone()), "{key:?}");
		}
	}

	#[test]
	fn test_encoding_through_any_key_is_byte_identical() {
		// Any drift writes a second on-disk key for a row that already exists under the old bytes.
		for (key, bytes) in all_probes() {
			assert_eq!(key.encode(), bytes, "{key:?}");
		}
	}

	#[test]
	fn test_probes_cover_every_live_kind() {
		// Without this a variant can be added to the enum and left out of encode and decode.
		let mut kinds: Vec<u8> = all_probes().iter().map(|(key, _)| key.kind() as u8).collect();
		kinds.sort_unstable();
		kinds.dedup();
		assert_eq!(kinds.len(), 87);
	}

	#[test]
	fn test_metric_sub_families_decode_into_their_own_variant() {
		// Both families share kind 0x22; the wrong pick loses the tier and reads another counter.
		let storage = MetricStorageKey::new(Tier::Persistent, MetricsId::System);
		let cdc = MetricCdcKey::new(MetricsId::System);
		assert_eq!(TaggedKey::decode(&storage.encode()), Some(TaggedKey::Metric(MetricKey::Storage(storage))));
		assert_eq!(TaggedKey::decode(&cdc.encode()), Some(TaggedKey::Metric(MetricKey::Cdc(cdc))));
	}

	#[test]
	fn test_decode_rejects_bytes_that_name_no_live_kind() {
		// 0x24-0x26 and 0x3F were removed; a hole must not resurrect as a neighbouring key.
		assert_eq!(TaggedKey::decode(&EncodedKey::new(Vec::<u8>::new())), None);
		for hole in [0x24u8, 0x25, 0x26, 0x3F] {
			assert_eq!(TaggedKey::decode(&EncodedKey::new(vec![!hole, 0x00, 0x01])), None);
		}
	}

	#[test]
	fn test_decode_rejects_a_truncated_payload() {
		// A short read must fail rather than hand back a key built from whatever bytes arrived.
		let bytes = RowKey {
			storage: StorageId::table(1),
			row: RowNumber(1),
		}
		.encode();
		let mut truncated = bytes.as_slice().to_vec();
		truncated.truncate(truncated.len() - 1);
		assert_eq!(TaggedKey::decode(&EncodedKey::new(truncated)), None);
	}

	#[test]
	fn test_index_entry_with_an_empty_suffix_round_trips() {
		// set_encoded decodes what it is handed, so a write-only key would error on write.
		let key = IndexEntryKey::new(
			ObjectId::table(1),
			IndexId::primary(PrimaryKeyId(1)),
			EncodedIndexKey::new([0u8; 0]),
		);
		let bytes = key.encode();
		assert_eq!(TaggedKey::from(key.clone()).encode(), bytes);
		assert_eq!(TaggedKey::decode(&bytes), Some(TaggedKey::from(key)));
	}

	#[test]
	fn test_any_key_fits_two_machine_words_beyond_an_encoded_key() {
		// Delta holds one key per pending write, so this number sets the per-transaction budget.
		assert_eq!(size_of::<EncodedKey>(), 48);
		assert_eq!(size_of::<TaggedKey>(), 96);
		assert_eq!(size_of::<PartitionedSortedViewRowKey>(), size_of::<TaggedKey>());
	}
}
