// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, cmp::Ordering};

use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_value::value::Value;
use smallvec::SmallVec;

use crate::{
	interface::{
		catalog::{id::IndexId, metrics::MetricsId, object::ObjectId, storage::StorageId},
		store::Tier,
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
			RowShapeFieldKey, RowShapeKey, SortedViewRowKey,
		},
		series::{PartitionedSeriesRowKey, SeriesKey, SeriesMetadataKey, SeriesRowKey},
		system::{
			MigrationEventKey, MigrationKey, SystemSequenceKey, SystemVersionKey, TransactionVersionKey,
			VersionEpochKey,
		},
		typed::key::Key,
	},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKey {
	Cdc(MetricCdcKey),
	Storage(MetricStorageKey),
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum AnyKey {
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

impl AnyKey {
	pub fn kind(&self) -> KeyKind {
		match self {
			Self::Namespace(_) => KeyKind::Namespace,
			Self::Table(_) => KeyKind::Table,
			Self::Row(_) => KeyKind::Row,
			Self::NamespaceTable(_) => KeyKind::NamespaceTable,
			Self::SystemSequence(_) => KeyKind::SystemSequence,
			Self::Columns(_) => KeyKind::Columns,
			Self::Column(_) => KeyKind::Column,
			Self::RowSequence(_) => KeyKind::RowSequence,
			Self::ColumnProperty(_) => KeyKind::ColumnProperty,
			Self::SystemVersion(_) => KeyKind::SystemVersion,
			Self::TransactionVersion(_) => KeyKind::TransactionVersion,
			Self::Index(_) => KeyKind::Index,
			Self::IndexEntry(_) => KeyKind::IndexEntry,
			Self::ColumnSequence(_) => KeyKind::ColumnSequence,
			Self::CdcConsumer(_) => KeyKind::CdcConsumer,
			Self::View(_) => KeyKind::View,
			Self::NamespaceView(_) => KeyKind::NamespaceView,
			Self::PrimaryKey(_) => KeyKind::PrimaryKey,
			Self::OperatorState(_) => KeyKind::OperatorState,
			Self::RingBuffer(_) => KeyKind::RingBuffer,
			Self::NamespaceRingBuffer(_) => KeyKind::NamespaceRingBuffer,
			Self::RingBufferMetadata(_) => KeyKind::RingBufferMetadata,
			Self::Flow(_) => KeyKind::Flow,
			Self::NamespaceFlow(_) => KeyKind::NamespaceFlow,
			Self::Operator(_) => KeyKind::Operator,
			Self::OperatorByFlow(_) => KeyKind::OperatorByFlow,
			Self::FlowEdge(_) => KeyKind::FlowEdge,
			Self::FlowEdgeByFlow(_) => KeyKind::FlowEdgeByFlow,
			Self::OutputFrontier(_) => KeyKind::OutputFrontier,
			Self::Dictionary(_) => KeyKind::Dictionary,
			Self::DictionaryEntry(_) => KeyKind::DictionaryEntry,
			Self::DictionaryEntryIndex(_) => KeyKind::DictionaryEntryIndex,
			Self::NamespaceDictionary(_) => KeyKind::NamespaceDictionary,
			Self::Metric(_) => KeyKind::Metric,
			Self::FlowVersion(_) => KeyKind::FlowVersion,
			Self::RowShape(_) => KeyKind::RowShape,
			Self::RowShapeField(_) => KeyKind::RowShapeField,
			Self::SumType(_) => KeyKind::SumType,
			Self::NamespaceSumType(_) => KeyKind::NamespaceSumType,
			Self::Handler(_) => KeyKind::Handler,
			Self::NamespaceHandler(_) => KeyKind::NamespaceHandler,
			Self::VariantHandler(_) => KeyKind::VariantHandler,
			Self::Series(_) => KeyKind::Series,
			Self::NamespaceSeries(_) => KeyKind::NamespaceSeries,
			Self::SeriesMetadata(_) => KeyKind::SeriesMetadata,
			Self::Identity(_) => KeyKind::Identity,
			Self::Role(_) => KeyKind::Role,
			Self::GrantedRole(_) => KeyKind::GrantedRole,
			Self::Policy(_) => KeyKind::Policy,
			Self::PolicyOp(_) => KeyKind::PolicyOp,
			Self::Migration(_) => KeyKind::Migration,
			Self::MigrationEvent(_) => KeyKind::MigrationEvent,
			Self::Authentication(_) => KeyKind::Authentication,
			Self::ConfigStorage(_) => KeyKind::ConfigStorage,
			Self::Token(_) => KeyKind::Token,
			Self::Source(_) => KeyKind::Source,
			Self::NamespaceSource(_) => KeyKind::NamespaceSource,
			Self::Sink(_) => KeyKind::Sink,
			Self::NamespaceSink(_) => KeyKind::NamespaceSink,
			Self::RowSettings(_) => KeyKind::RowSettings,
			Self::Procedure(_) => KeyKind::Procedure,
			Self::NamespaceProcedure(_) => KeyKind::NamespaceProcedure,
			Self::ProcedureParam(_) => KeyKind::ProcedureParam,
			Self::Binding(_) => KeyKind::Binding,
			Self::NamespaceBinding(_) => KeyKind::NamespaceBinding,
			Self::OperatorSettings(_) => KeyKind::OperatorSettings,
			Self::ColumnSnapshot(_) => KeyKind::ColumnSnapshot,
			Self::SeriesColumnSnapshot(_) => KeyKind::SeriesColumnSnapshot,
			Self::TableColumnSnapshot(_) => KeyKind::TableColumnSnapshot,
			Self::VersionEpoch(_) => KeyKind::VersionEpoch,
			Self::IdentityAttribute(_) => KeyKind::IdentityAttribute,
			Self::IdentityAttributeValue(_) => KeyKind::IdentityAttributeValue,
			Self::PartitionedRow(_) => KeyKind::PartitionedRow,
			Self::Partition(_) => KeyKind::Partition,
			Self::Queue(_) => KeyKind::Queue,
			Self::NamespaceQueue(_) => KeyKind::NamespaceQueue,
			Self::QueueDeduplication(_) => KeyKind::QueueDeduplication,
			Self::Relationship(_) => KeyKind::Relationship,
			Self::SeriesRow(_) => KeyKind::SeriesRow,
			Self::PartitionedSeriesRow(_) => KeyKind::PartitionedSeriesRow,
			Self::QueuePartition(_) => KeyKind::QueuePartition,
			Self::QueueItemState(_) => KeyKind::QueueItemState,
			Self::QueueDue(_) => KeyKind::QueueDue,
			Self::QueueAttempt(_) => KeyKind::QueueAttempt,
			Self::QueueKeyActive(_) => KeyKind::QueueKeyActive,
			Self::SortedViewRow(_) => KeyKind::SortedViewRow,
			Self::PartitionedSortedViewRow(_) => KeyKind::PartitionedSortedViewRow,
		}
	}

	pub fn encode(&self) -> EncodedKey {
		match self {
			Self::Namespace(key) => Key::encode(key),
			Self::Table(key) => Key::encode(key),
			Self::Row(key) => Key::encode(key),
			Self::NamespaceTable(key) => Key::encode(key),
			Self::SystemSequence(key) => Key::encode(key),
			Self::Columns(key) => Key::encode(key),
			Self::Column(key) => Key::encode(key),
			Self::RowSequence(key) => Key::encode(key),
			Self::ColumnProperty(key) => Key::encode(key),
			Self::SystemVersion(key) => Key::encode(key),
			Self::TransactionVersion(key) => Key::encode(key),
			Self::Index(key) => Key::encode(key),
			Self::IndexEntry(key) => EncodableKey::encode(key),
			Self::ColumnSequence(key) => Key::encode(key),
			Self::CdcConsumer(key) => EncodableKey::encode(key),
			Self::View(key) => Key::encode(key),
			Self::NamespaceView(key) => Key::encode(key),
			Self::PrimaryKey(key) => Key::encode(key),
			Self::OperatorState(key) => EncodableKey::encode(key),
			Self::RingBuffer(key) => Key::encode(key),
			Self::NamespaceRingBuffer(key) => Key::encode(key),
			Self::RingBufferMetadata(key) => EncodableKey::encode(key),
			Self::Flow(key) => Key::encode(key),
			Self::NamespaceFlow(key) => Key::encode(key),
			Self::Operator(key) => Key::encode(key),
			Self::OperatorByFlow(key) => Key::encode(key),
			Self::FlowEdge(key) => Key::encode(key),
			Self::FlowEdgeByFlow(key) => Key::encode(key),
			Self::OutputFrontier(key) => Key::encode(key),
			Self::Dictionary(key) => Key::encode(key),
			Self::DictionaryEntry(key) => Key::encode(key),
			Self::DictionaryEntryIndex(key) => EncodableKey::encode(key),
			Self::NamespaceDictionary(key) => Key::encode(key),
			Self::Metric(MetricKey::Cdc(key)) => Key::encode(key),
			Self::Metric(MetricKey::Storage(key)) => Key::encode(key),
			Self::FlowVersion(key) => Key::encode(key),
			Self::RowShape(key) => Key::encode(key),
			Self::RowShapeField(key) => Key::encode(key),
			Self::SumType(key) => Key::encode(key),
			Self::NamespaceSumType(key) => Key::encode(key),
			Self::Handler(key) => Key::encode(key),
			Self::NamespaceHandler(key) => Key::encode(key),
			Self::VariantHandler(key) => Key::encode(key),
			Self::Series(key) => Key::encode(key),
			Self::NamespaceSeries(key) => Key::encode(key),
			Self::SeriesMetadata(key) => Key::encode(key),
			Self::Identity(key) => Key::encode(key),
			Self::Role(key) => Key::encode(key),
			Self::GrantedRole(key) => Key::encode(key),
			Self::Policy(key) => Key::encode(key),
			Self::PolicyOp(key) => Key::encode(key),
			Self::Migration(key) => Key::encode(key),
			Self::MigrationEvent(key) => Key::encode(key),
			Self::Authentication(key) => Key::encode(key),
			Self::ConfigStorage(key) => EncodableKey::encode(key),
			Self::Token(key) => Key::encode(key),
			Self::Source(key) => Key::encode(key),
			Self::NamespaceSource(key) => Key::encode(key),
			Self::Sink(key) => Key::encode(key),
			Self::NamespaceSink(key) => Key::encode(key),
			Self::RowSettings(key) => Key::encode(key),
			Self::Procedure(key) => Key::encode(key),
			Self::NamespaceProcedure(key) => Key::encode(key),
			Self::ProcedureParam(key) => Key::encode(key),
			Self::Binding(key) => Key::encode(key),
			Self::NamespaceBinding(key) => Key::encode(key),
			Self::OperatorSettings(key) => Key::encode(key),
			Self::ColumnSnapshot(key) => Key::encode(key),
			Self::SeriesColumnSnapshot(key) => Key::encode(key),
			Self::TableColumnSnapshot(key) => Key::encode(key),
			Self::VersionEpoch(key) => Key::encode(key),
			Self::IdentityAttribute(key) => Key::encode(key),
			Self::IdentityAttributeValue(key) => Key::encode(key),
			Self::PartitionedRow(key) => Key::encode(key),
			Self::Partition(key) => Key::encode(key),
			Self::Queue(key) => Key::encode(key),
			Self::NamespaceQueue(key) => Key::encode(key),
			Self::QueueDeduplication(key) => EncodableKey::encode(key),
			Self::Relationship(key) => Key::encode(key),
			Self::SeriesRow(key) => Key::encode(key),
			Self::PartitionedSeriesRow(key) => Key::encode(key),
			Self::QueuePartition(key) => Key::encode(key),
			Self::QueueItemState(key) => Key::encode(key),
			Self::QueueDue(key) => Key::encode(key),
			Self::QueueAttempt(key) => Key::encode(key),
			Self::QueueKeyActive(key) => Key::encode(key),
			Self::SortedViewRow(key) => Key::encode(key),
			Self::PartitionedSortedViewRow(key) => Key::encode(key),
		}
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		match KeyKind::of(key)? {
			KeyKind::Namespace => Key::decode(key).map(Self::Namespace),
			KeyKind::Table => Key::decode(key).map(Self::Table),
			KeyKind::Row => Key::decode(key).map(Self::Row),
			KeyKind::NamespaceTable => Key::decode(key).map(Self::NamespaceTable),
			KeyKind::SystemSequence => Key::decode(key).map(Self::SystemSequence),
			KeyKind::Columns => Key::decode(key).map(Self::Columns),
			KeyKind::Column => Key::decode(key).map(Self::Column),
			KeyKind::RowSequence => Key::decode(key).map(Self::RowSequence),
			KeyKind::ColumnProperty => Key::decode(key).map(Self::ColumnProperty),
			KeyKind::SystemVersion => Key::decode(key).map(Self::SystemVersion),
			KeyKind::TransactionVersion => Key::decode(key).map(Self::TransactionVersion),
			KeyKind::Index => Key::decode(key).map(Self::Index),
			KeyKind::IndexEntry => EncodableKey::decode(key).map(Self::IndexEntry),
			KeyKind::ColumnSequence => Key::decode(key).map(Self::ColumnSequence),
			KeyKind::CdcConsumer => EncodableKey::decode(key).map(Self::CdcConsumer),
			KeyKind::View => Key::decode(key).map(Self::View),
			KeyKind::NamespaceView => Key::decode(key).map(Self::NamespaceView),
			KeyKind::PrimaryKey => Key::decode(key).map(Self::PrimaryKey),
			KeyKind::OperatorState => EncodableKey::decode(key).map(Self::OperatorState),
			KeyKind::RingBuffer => Key::decode(key).map(Self::RingBuffer),
			KeyKind::NamespaceRingBuffer => Key::decode(key).map(Self::NamespaceRingBuffer),
			KeyKind::RingBufferMetadata => EncodableKey::decode(key).map(Self::RingBufferMetadata),
			KeyKind::Flow => Key::decode(key).map(Self::Flow),
			KeyKind::NamespaceFlow => Key::decode(key).map(Self::NamespaceFlow),
			KeyKind::Operator => Key::decode(key).map(Self::Operator),
			KeyKind::OperatorByFlow => Key::decode(key).map(Self::OperatorByFlow),
			KeyKind::FlowEdge => Key::decode(key).map(Self::FlowEdge),
			KeyKind::FlowEdgeByFlow => Key::decode(key).map(Self::FlowEdgeByFlow),
			KeyKind::OutputFrontier => Key::decode(key).map(Self::OutputFrontier),
			KeyKind::Dictionary => Key::decode(key).map(Self::Dictionary),
			KeyKind::DictionaryEntry => Key::decode(key).map(Self::DictionaryEntry),
			KeyKind::DictionaryEntryIndex => EncodableKey::decode(key).map(Self::DictionaryEntryIndex),
			KeyKind::NamespaceDictionary => Key::decode(key).map(Self::NamespaceDictionary),
			KeyKind::Metric => decode_metric(key),
			KeyKind::FlowVersion => Key::decode(key).map(Self::FlowVersion),
			KeyKind::RowShape => Key::decode(key).map(Self::RowShape),
			KeyKind::RowShapeField => Key::decode(key).map(Self::RowShapeField),
			KeyKind::SumType => Key::decode(key).map(Self::SumType),
			KeyKind::NamespaceSumType => Key::decode(key).map(Self::NamespaceSumType),
			KeyKind::Handler => Key::decode(key).map(Self::Handler),
			KeyKind::NamespaceHandler => Key::decode(key).map(Self::NamespaceHandler),
			KeyKind::VariantHandler => Key::decode(key).map(Self::VariantHandler),
			KeyKind::Series => Key::decode(key).map(Self::Series),
			KeyKind::NamespaceSeries => Key::decode(key).map(Self::NamespaceSeries),
			KeyKind::SeriesMetadata => Key::decode(key).map(Self::SeriesMetadata),
			KeyKind::Identity => Key::decode(key).map(Self::Identity),
			KeyKind::Role => Key::decode(key).map(Self::Role),
			KeyKind::GrantedRole => Key::decode(key).map(Self::GrantedRole),
			KeyKind::Policy => Key::decode(key).map(Self::Policy),
			KeyKind::PolicyOp => Key::decode(key).map(Self::PolicyOp),
			KeyKind::Migration => Key::decode(key).map(Self::Migration),
			KeyKind::MigrationEvent => Key::decode(key).map(Self::MigrationEvent),
			KeyKind::Authentication => Key::decode(key).map(Self::Authentication),
			KeyKind::ConfigStorage => EncodableKey::decode(key).map(Self::ConfigStorage),
			KeyKind::Token => Key::decode(key).map(Self::Token),
			KeyKind::Source => Key::decode(key).map(Self::Source),
			KeyKind::NamespaceSource => Key::decode(key).map(Self::NamespaceSource),
			KeyKind::Sink => Key::decode(key).map(Self::Sink),
			KeyKind::NamespaceSink => Key::decode(key).map(Self::NamespaceSink),
			KeyKind::RowSettings => Key::decode(key).map(Self::RowSettings),
			KeyKind::Procedure => Key::decode(key).map(Self::Procedure),
			KeyKind::NamespaceProcedure => Key::decode(key).map(Self::NamespaceProcedure),
			KeyKind::ProcedureParam => Key::decode(key).map(Self::ProcedureParam),
			KeyKind::Binding => Key::decode(key).map(Self::Binding),
			KeyKind::NamespaceBinding => Key::decode(key).map(Self::NamespaceBinding),
			KeyKind::OperatorSettings => Key::decode(key).map(Self::OperatorSettings),
			KeyKind::ColumnSnapshot => Key::decode(key).map(Self::ColumnSnapshot),
			KeyKind::SeriesColumnSnapshot => Key::decode(key).map(Self::SeriesColumnSnapshot),
			KeyKind::TableColumnSnapshot => Key::decode(key).map(Self::TableColumnSnapshot),
			KeyKind::VersionEpoch => Key::decode(key).map(Self::VersionEpoch),
			KeyKind::IdentityAttribute => Key::decode(key).map(Self::IdentityAttribute),
			KeyKind::IdentityAttributeValue => Key::decode(key).map(Self::IdentityAttributeValue),
			KeyKind::PartitionedRow => Key::decode(key).map(Self::PartitionedRow),
			KeyKind::Partition => Key::decode(key).map(Self::Partition),
			KeyKind::Queue => Key::decode(key).map(Self::Queue),
			KeyKind::NamespaceQueue => Key::decode(key).map(Self::NamespaceQueue),
			KeyKind::QueueDeduplication => EncodableKey::decode(key).map(Self::QueueDeduplication),
			KeyKind::Relationship => Key::decode(key).map(Self::Relationship),
			KeyKind::SeriesRow => Key::decode(key).map(Self::SeriesRow),
			KeyKind::PartitionedSeriesRow => Key::decode(key).map(Self::PartitionedSeriesRow),
			KeyKind::QueuePartition => Key::decode(key).map(Self::QueuePartition),
			KeyKind::QueueItemState => Key::decode(key).map(Self::QueueItemState),
			KeyKind::QueueDue => Key::decode(key).map(Self::QueueDue),
			KeyKind::QueueAttempt => Key::decode(key).map(Self::QueueAttempt),
			KeyKind::QueueKeyActive => Key::decode(key).map(Self::QueueKeyActive),
			KeyKind::SortedViewRow => Key::decode(key).map(Self::SortedViewRow),
			KeyKind::PartitionedSortedViewRow => Key::decode(key).map(Self::PartitionedSortedViewRow),
		}
	}
}

fn decode_metric(key: &EncodedKey) -> Option<AnyKey> {
	Key::decode(key).map(MetricKey::Storage).or_else(|| Key::decode(key).map(MetricKey::Cdc)).map(AnyKey::Metric)
}

impl From<NamespaceKey> for AnyKey {
	fn from(key: NamespaceKey) -> Self {
		Self::Namespace(key)
	}
}

impl From<TableKey> for AnyKey {
	fn from(key: TableKey) -> Self {
		Self::Table(key)
	}
}

impl From<RowKey> for AnyKey {
	fn from(key: RowKey) -> Self {
		Self::Row(key)
	}
}

impl From<NamespaceTableKey> for AnyKey {
	fn from(key: NamespaceTableKey) -> Self {
		Self::NamespaceTable(key)
	}
}

impl From<SystemSequenceKey> for AnyKey {
	fn from(key: SystemSequenceKey) -> Self {
		Self::SystemSequence(key)
	}
}

impl From<ColumnsKey> for AnyKey {
	fn from(key: ColumnsKey) -> Self {
		Self::Columns(key)
	}
}

impl From<ColumnKey> for AnyKey {
	fn from(key: ColumnKey) -> Self {
		Self::Column(key)
	}
}

impl From<RowSequenceKey> for AnyKey {
	fn from(key: RowSequenceKey) -> Self {
		Self::RowSequence(key)
	}
}

impl From<ColumnPropertyKey> for AnyKey {
	fn from(key: ColumnPropertyKey) -> Self {
		Self::ColumnProperty(key)
	}
}

impl From<SystemVersionKey> for AnyKey {
	fn from(key: SystemVersionKey) -> Self {
		Self::SystemVersion(key)
	}
}

impl From<TransactionVersionKey> for AnyKey {
	fn from(key: TransactionVersionKey) -> Self {
		Self::TransactionVersion(key)
	}
}

impl From<IndexKey> for AnyKey {
	fn from(key: IndexKey) -> Self {
		Self::Index(key)
	}
}

impl From<IndexEntryKey> for AnyKey {
	fn from(key: IndexEntryKey) -> Self {
		Self::IndexEntry(key)
	}
}

impl From<ColumnSequenceKey> for AnyKey {
	fn from(key: ColumnSequenceKey) -> Self {
		Self::ColumnSequence(key)
	}
}

impl From<CdcConsumerKey> for AnyKey {
	fn from(key: CdcConsumerKey) -> Self {
		Self::CdcConsumer(key)
	}
}

impl From<ViewKey> for AnyKey {
	fn from(key: ViewKey) -> Self {
		Self::View(key)
	}
}

impl From<NamespaceViewKey> for AnyKey {
	fn from(key: NamespaceViewKey) -> Self {
		Self::NamespaceView(key)
	}
}

impl From<PrimaryKeyKey> for AnyKey {
	fn from(key: PrimaryKeyKey) -> Self {
		Self::PrimaryKey(key)
	}
}

impl From<OperatorStateKey> for AnyKey {
	fn from(key: OperatorStateKey) -> Self {
		Self::OperatorState(key)
	}
}

impl From<RingBufferKey> for AnyKey {
	fn from(key: RingBufferKey) -> Self {
		Self::RingBuffer(key)
	}
}

impl From<NamespaceRingBufferKey> for AnyKey {
	fn from(key: NamespaceRingBufferKey) -> Self {
		Self::NamespaceRingBuffer(key)
	}
}

impl From<RingBufferMetadataKey> for AnyKey {
	fn from(key: RingBufferMetadataKey) -> Self {
		Self::RingBufferMetadata(key)
	}
}

impl From<FlowKey> for AnyKey {
	fn from(key: FlowKey) -> Self {
		Self::Flow(key)
	}
}

impl From<NamespaceFlowKey> for AnyKey {
	fn from(key: NamespaceFlowKey) -> Self {
		Self::NamespaceFlow(key)
	}
}

impl From<OperatorKey> for AnyKey {
	fn from(key: OperatorKey) -> Self {
		Self::Operator(key)
	}
}

impl From<OperatorByFlowKey> for AnyKey {
	fn from(key: OperatorByFlowKey) -> Self {
		Self::OperatorByFlow(key)
	}
}

impl From<FlowEdgeKey> for AnyKey {
	fn from(key: FlowEdgeKey) -> Self {
		Self::FlowEdge(key)
	}
}

impl From<FlowEdgeByFlowKey> for AnyKey {
	fn from(key: FlowEdgeByFlowKey) -> Self {
		Self::FlowEdgeByFlow(key)
	}
}

impl From<OutputFrontierKey> for AnyKey {
	fn from(key: OutputFrontierKey) -> Self {
		Self::OutputFrontier(key)
	}
}

impl From<DictionaryKey> for AnyKey {
	fn from(key: DictionaryKey) -> Self {
		Self::Dictionary(key)
	}
}

impl From<DictionaryEntryKey> for AnyKey {
	fn from(key: DictionaryEntryKey) -> Self {
		Self::DictionaryEntry(key)
	}
}

impl From<DictionaryEntryIndexKey> for AnyKey {
	fn from(key: DictionaryEntryIndexKey) -> Self {
		Self::DictionaryEntryIndex(key)
	}
}

impl From<NamespaceDictionaryKey> for AnyKey {
	fn from(key: NamespaceDictionaryKey) -> Self {
		Self::NamespaceDictionary(key)
	}
}

impl From<FlowVersionKey> for AnyKey {
	fn from(key: FlowVersionKey) -> Self {
		Self::FlowVersion(key)
	}
}

impl From<RowShapeKey> for AnyKey {
	fn from(key: RowShapeKey) -> Self {
		Self::RowShape(key)
	}
}

impl From<RowShapeFieldKey> for AnyKey {
	fn from(key: RowShapeFieldKey) -> Self {
		Self::RowShapeField(key)
	}
}

impl From<SumTypeKey> for AnyKey {
	fn from(key: SumTypeKey) -> Self {
		Self::SumType(key)
	}
}

impl From<NamespaceSumTypeKey> for AnyKey {
	fn from(key: NamespaceSumTypeKey) -> Self {
		Self::NamespaceSumType(key)
	}
}

impl From<HandlerKey> for AnyKey {
	fn from(key: HandlerKey) -> Self {
		Self::Handler(key)
	}
}

impl From<NamespaceHandlerKey> for AnyKey {
	fn from(key: NamespaceHandlerKey) -> Self {
		Self::NamespaceHandler(key)
	}
}

impl From<VariantHandlerKey> for AnyKey {
	fn from(key: VariantHandlerKey) -> Self {
		Self::VariantHandler(key)
	}
}

impl From<SeriesKey> for AnyKey {
	fn from(key: SeriesKey) -> Self {
		Self::Series(key)
	}
}

impl From<NamespaceSeriesKey> for AnyKey {
	fn from(key: NamespaceSeriesKey) -> Self {
		Self::NamespaceSeries(key)
	}
}

impl From<SeriesMetadataKey> for AnyKey {
	fn from(key: SeriesMetadataKey) -> Self {
		Self::SeriesMetadata(key)
	}
}

impl From<IdentityKey> for AnyKey {
	fn from(key: IdentityKey) -> Self {
		Self::Identity(key)
	}
}

impl From<RoleKey> for AnyKey {
	fn from(key: RoleKey) -> Self {
		Self::Role(key)
	}
}

impl From<GrantedRoleKey> for AnyKey {
	fn from(key: GrantedRoleKey) -> Self {
		Self::GrantedRole(key)
	}
}

impl From<PolicyKey> for AnyKey {
	fn from(key: PolicyKey) -> Self {
		Self::Policy(key)
	}
}

impl From<PolicyOpKey> for AnyKey {
	fn from(key: PolicyOpKey) -> Self {
		Self::PolicyOp(key)
	}
}

impl From<MigrationKey> for AnyKey {
	fn from(key: MigrationKey) -> Self {
		Self::Migration(key)
	}
}

impl From<MigrationEventKey> for AnyKey {
	fn from(key: MigrationEventKey) -> Self {
		Self::MigrationEvent(key)
	}
}

impl From<AuthenticationKey> for AnyKey {
	fn from(key: AuthenticationKey) -> Self {
		Self::Authentication(key)
	}
}

impl From<ConfigStorageKey> for AnyKey {
	fn from(key: ConfigStorageKey) -> Self {
		Self::ConfigStorage(key)
	}
}

impl From<TokenKey> for AnyKey {
	fn from(key: TokenKey) -> Self {
		Self::Token(key)
	}
}

impl From<SourceKey> for AnyKey {
	fn from(key: SourceKey) -> Self {
		Self::Source(key)
	}
}

impl From<NamespaceSourceKey> for AnyKey {
	fn from(key: NamespaceSourceKey) -> Self {
		Self::NamespaceSource(key)
	}
}

impl From<SinkKey> for AnyKey {
	fn from(key: SinkKey) -> Self {
		Self::Sink(key)
	}
}

impl From<NamespaceSinkKey> for AnyKey {
	fn from(key: NamespaceSinkKey) -> Self {
		Self::NamespaceSink(key)
	}
}

impl From<RowSettingsKey> for AnyKey {
	fn from(key: RowSettingsKey) -> Self {
		Self::RowSettings(key)
	}
}

impl From<ProcedureKey> for AnyKey {
	fn from(key: ProcedureKey) -> Self {
		Self::Procedure(key)
	}
}

impl From<NamespaceProcedureKey> for AnyKey {
	fn from(key: NamespaceProcedureKey) -> Self {
		Self::NamespaceProcedure(key)
	}
}

impl From<ProcedureParamKey> for AnyKey {
	fn from(key: ProcedureParamKey) -> Self {
		Self::ProcedureParam(key)
	}
}

impl From<BindingKey> for AnyKey {
	fn from(key: BindingKey) -> Self {
		Self::Binding(key)
	}
}

impl From<NamespaceBindingKey> for AnyKey {
	fn from(key: NamespaceBindingKey) -> Self {
		Self::NamespaceBinding(key)
	}
}

impl From<OperatorSettingsKey> for AnyKey {
	fn from(key: OperatorSettingsKey) -> Self {
		Self::OperatorSettings(key)
	}
}

impl From<ColumnSnapshotKey> for AnyKey {
	fn from(key: ColumnSnapshotKey) -> Self {
		Self::ColumnSnapshot(key)
	}
}

impl From<SeriesColumnSnapshotKey> for AnyKey {
	fn from(key: SeriesColumnSnapshotKey) -> Self {
		Self::SeriesColumnSnapshot(key)
	}
}

impl From<TableColumnSnapshotKey> for AnyKey {
	fn from(key: TableColumnSnapshotKey) -> Self {
		Self::TableColumnSnapshot(key)
	}
}

impl From<VersionEpochKey> for AnyKey {
	fn from(key: VersionEpochKey) -> Self {
		Self::VersionEpoch(key)
	}
}

impl From<IdentityAttributeKey> for AnyKey {
	fn from(key: IdentityAttributeKey) -> Self {
		Self::IdentityAttribute(key)
	}
}

impl From<IdentityAttributeValueKey> for AnyKey {
	fn from(key: IdentityAttributeValueKey) -> Self {
		Self::IdentityAttributeValue(key)
	}
}

impl From<PartitionedRowKey> for AnyKey {
	fn from(key: PartitionedRowKey) -> Self {
		Self::PartitionedRow(key)
	}
}

impl From<PartitionKey> for AnyKey {
	fn from(key: PartitionKey) -> Self {
		Self::Partition(key)
	}
}

impl From<QueueKey> for AnyKey {
	fn from(key: QueueKey) -> Self {
		Self::Queue(key)
	}
}

impl From<NamespaceQueueKey> for AnyKey {
	fn from(key: NamespaceQueueKey) -> Self {
		Self::NamespaceQueue(key)
	}
}

impl From<QueueDeduplicationKey> for AnyKey {
	fn from(key: QueueDeduplicationKey) -> Self {
		Self::QueueDeduplication(key)
	}
}

impl From<RelationshipKey> for AnyKey {
	fn from(key: RelationshipKey) -> Self {
		Self::Relationship(key)
	}
}

impl From<SeriesRowKey> for AnyKey {
	fn from(key: SeriesRowKey) -> Self {
		Self::SeriesRow(key)
	}
}

impl From<PartitionedSeriesRowKey> for AnyKey {
	fn from(key: PartitionedSeriesRowKey) -> Self {
		Self::PartitionedSeriesRow(key)
	}
}

impl From<QueuePartitionKey> for AnyKey {
	fn from(key: QueuePartitionKey) -> Self {
		Self::QueuePartition(key)
	}
}

impl From<QueueItemStateKey> for AnyKey {
	fn from(key: QueueItemStateKey) -> Self {
		Self::QueueItemState(key)
	}
}

impl From<QueueDueKey> for AnyKey {
	fn from(key: QueueDueKey) -> Self {
		Self::QueueDue(key)
	}
}

impl From<QueueAttemptKey> for AnyKey {
	fn from(key: QueueAttemptKey) -> Self {
		Self::QueueAttempt(key)
	}
}

impl From<QueueKeyActiveKey> for AnyKey {
	fn from(key: QueueKeyActiveKey) -> Self {
		Self::QueueKeyActive(key)
	}
}

impl From<SortedViewRowKey> for AnyKey {
	fn from(key: SortedViewRowKey) -> Self {
		Self::SortedViewRow(key)
	}
}

impl From<PartitionedSortedViewRowKey> for AnyKey {
	fn from(key: PartitionedSortedViewRowKey) -> Self {
		Self::PartitionedSortedViewRow(key)
	}
}

impl From<MetricKey> for AnyKey {
	fn from(key: MetricKey) -> Self {
		Self::Metric(key)
	}
}

impl From<MetricStorageKey> for AnyKey {
	fn from(key: MetricStorageKey) -> Self {
		Self::Metric(MetricKey::Storage(key))
	}
}

impl From<MetricCdcKey> for AnyKey {
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

fn storage_cmp(left: &StorageId, right: &StorageId) -> Ordering {
	object_cmp(&ObjectId::from(*left), &ObjectId::from(*right))
}

fn index_tag(index: &IndexId) -> u8 {
	match index {
		IndexId::Primary(_) => 0x01,
	}
}

fn tagged_index_cmp(left: &IndexId, right: &IndexId) -> Ordering {
	index_tag(left).cmp(&index_tag(right)).then_with(|| right.as_u64().cmp(&left.as_u64()))
}

fn option_u8_cmp(left: &Option<u8>, right: &Option<u8>) -> Ordering {
	match (left, right) {
		(Some(left), Some(right)) => right.cmp(left),
		(Some(_), None) => Ordering::Less,
		(None, Some(_)) => Ordering::Greater,
		(None, None) => Ordering::Equal,
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

fn encode_values(values: &[Value]) -> EncodedKey {
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

impl Eq for AnyKey {}

#[derive(Debug, Clone)]
pub enum Field<'a> {
	U8Asc(u8),
	UDesc(u128),
	BytesDesc(Cow<'a, [u8]>),
	RawAsc(&'a [u8]),
}

impl Field<'_> {
	fn variant_rank(&self) -> u8 {
		match self {
			Self::U8Asc(_) => 0,
			Self::UDesc(_) => 1,
			Self::BytesDesc(_) => 2,
			Self::RawAsc(_) => 3,
		}
	}
}

impl Ord for Field<'_> {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Self::U8Asc(left), Self::U8Asc(right)) => left.cmp(right),
			(Self::UDesc(left), Self::UDesc(right)) => right.cmp(left),
			(Self::BytesDesc(left), Self::BytesDesc(right)) => right.as_ref().cmp(left.as_ref()),
			(Self::RawAsc(left), Self::RawAsc(right)) => left.cmp(right),
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
	fn fields(&self) -> SmallVec<[Field<'_>; 4]>;
}

impl Ord for AnyKey {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Self::Namespace(a), Self::Namespace(b)) => desc(&a.namespace, &b.namespace),
			(Self::Table(a), Self::Table(b)) => desc(&a.table, &b.table),
			(Self::Row(a), Self::Row(b)) => {
				storage_cmp(&a.storage, &b.storage).then_with(|| desc(&a.row, &b.row))
			}
			(Self::NamespaceTable(a), Self::NamespaceTable(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.table, &b.table))
			}
			(Self::SystemSequence(a), Self::SystemSequence(b)) => desc(&a.sequence, &b.sequence),
			(Self::Columns(a), Self::Columns(b)) => desc(&a.column, &b.column),
			(Self::Column(a), Self::Column(b)) => {
				object_cmp(&a.object, &b.object).then_with(|| desc(&a.column, &b.column))
			}
			(Self::RowSequence(a), Self::RowSequence(b)) => storage_cmp(&a.storage, &b.storage),
			(Self::ColumnProperty(a), Self::ColumnProperty(b)) => {
				desc(&a.column, &b.column).then_with(|| desc(&a.property, &b.property))
			}
			(Self::SystemVersion(a), Self::SystemVersion(b)) => {
				desc(&(a.version as u8), &(b.version as u8))
			}
			(Self::TransactionVersion(_), Self::TransactionVersion(_)) => Ordering::Equal,
			(Self::Index(a), Self::Index(b)) => object_cmp(&a.object, &b.object)
				.then_with(|| desc(&a.index.as_u64(), &b.index.as_u64())),
			(Self::IndexEntry(a), Self::IndexEntry(b)) => object_cmp(&a.object, &b.object)
				.then_with(|| tagged_index_cmp(&a.index, &b.index))
				.then_with(|| a.key.cmp(&b.key)),
			(Self::ColumnSequence(a), Self::ColumnSequence(b)) => {
				object_cmp(&a.object, &b.object).then_with(|| desc(&a.column, &b.column))
			}
			(Self::CdcConsumer(a), Self::CdcConsumer(b)) => desc(&a.consumer, &b.consumer),
			(Self::View(a), Self::View(b)) => desc(&a.view, &b.view),
			(Self::NamespaceView(a), Self::NamespaceView(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.view, &b.view))
			}
			(Self::PrimaryKey(a), Self::PrimaryKey(b)) => desc(&a.primary_key, &b.primary_key),
			(Self::OperatorState(a), Self::OperatorState(b)) => desc(&a.operator, &b.operator)
				.then_with(|| desc(a.group.as_bytes(), b.group.as_bytes()))
				.then_with(|| desc(&a.keyspace, &b.keyspace))
				.then_with(|| a.suffix.cmp(&b.suffix)),
			(Self::RingBuffer(a), Self::RingBuffer(b)) => desc(&a.ringbuffer, &b.ringbuffer),
			(Self::NamespaceRingBuffer(a), Self::NamespaceRingBuffer(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.ringbuffer, &b.ringbuffer))
			}
			(Self::RingBufferMetadata(a), Self::RingBufferMetadata(b)) => {
				storage_cmp(&a.storage, &b.storage).then_with(|| {
					encode_values(&a.partition_values).cmp(&encode_values(&b.partition_values))
				})
			}
			(Self::Flow(a), Self::Flow(b)) => desc(&a.flow, &b.flow),
			(Self::NamespaceFlow(a), Self::NamespaceFlow(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.flow, &b.flow))
			}
			(Self::Operator(a), Self::Operator(b)) => desc(&a.operator, &b.operator),
			(Self::OperatorByFlow(a), Self::OperatorByFlow(b)) => {
				desc(&a.flow, &b.flow).then_with(|| desc(&a.operator, &b.operator))
			}
			(Self::FlowEdge(a), Self::FlowEdge(b)) => desc(&a.edge, &b.edge),
			(Self::FlowEdgeByFlow(a), Self::FlowEdgeByFlow(b)) => {
				desc(&a.flow, &b.flow).then_with(|| desc(&a.edge, &b.edge))
			}
			(Self::OutputFrontier(a), Self::OutputFrontier(b)) => object_cmp(&a.object, &b.object),
			(Self::Dictionary(a), Self::Dictionary(b)) => desc(&a.dictionary, &b.dictionary),
			(Self::DictionaryEntry(a), Self::DictionaryEntry(b)) => {
				desc(&a.dictionary, &b.dictionary).then_with(|| a.hash.cmp(&b.hash))
			}
			(Self::DictionaryEntryIndex(a), Self::DictionaryEntryIndex(b)) => {
				desc(&a.dictionary, &b.dictionary).then_with(|| desc(&a.id, &b.id))
			}
			(Self::NamespaceDictionary(a), Self::NamespaceDictionary(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.dictionary, &b.dictionary))
			}
			(Self::Metric(a), Self::Metric(b)) => a.cmp(b),
			(Self::FlowVersion(a), Self::FlowVersion(b)) => desc(&a.flow, &b.flow),
			(Self::RowShape(a), Self::RowShape(b)) => desc(&a.fingerprint, &b.fingerprint),
			(Self::RowShapeField(a), Self::RowShapeField(b)) => {
				desc(&a.shape_fingerprint, &b.shape_fingerprint)
					.then_with(|| desc(&a.field_index, &b.field_index))
			}
			(Self::SumType(a), Self::SumType(b)) => desc(&a.sumtype, &b.sumtype),
			(Self::NamespaceSumType(a), Self::NamespaceSumType(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.sumtype, &b.sumtype))
			}
			(Self::Handler(a), Self::Handler(b)) => desc(&a.handler, &b.handler),
			(Self::NamespaceHandler(a), Self::NamespaceHandler(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.handler, &b.handler))
			}
			(Self::VariantHandler(a), Self::VariantHandler(b)) => desc(&a.namespace, &b.namespace)
				.then_with(|| desc(&a.sumtype, &b.sumtype))
				.then_with(|| desc(&a.variant_tag, &b.variant_tag))
				.then_with(|| desc(&a.handler, &b.handler)),
			(Self::Series(a), Self::Series(b)) => desc(&a.series, &b.series),
			(Self::NamespaceSeries(a), Self::NamespaceSeries(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.series, &b.series))
			}
			(Self::SeriesMetadata(a), Self::SeriesMetadata(b)) => storage_cmp(&a.storage, &b.storage),
			(Self::Identity(a), Self::Identity(b)) => desc(&a.identity, &b.identity),
			(Self::Role(a), Self::Role(b)) => desc(&a.role, &b.role),
			(Self::GrantedRole(a), Self::GrantedRole(b)) => {
				desc(&a.identity, &b.identity).then_with(|| desc(&a.role, &b.role))
			}
			(Self::Policy(a), Self::Policy(b)) => desc(&a.policy, &b.policy),
			(Self::PolicyOp(a), Self::PolicyOp(b)) => {
				desc(&a.policy, &b.policy).then_with(|| desc(&a.op_index, &b.op_index))
			}
			(Self::Migration(a), Self::Migration(b)) => desc(&a.migration, &b.migration),
			(Self::MigrationEvent(a), Self::MigrationEvent(b)) => desc(&a.event, &b.event),
			(Self::Authentication(a), Self::Authentication(b)) => {
				desc(&a.authentication, &b.authentication)
			}
			(Self::ConfigStorage(a), Self::ConfigStorage(b)) => {
				desc(&a.key.to_string(), &b.key.to_string())
			}
			(Self::Token(a), Self::Token(b)) => desc(&a.token, &b.token),
			(Self::Source(a), Self::Source(b)) => desc(&a.source, &b.source),
			(Self::NamespaceSource(a), Self::NamespaceSource(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.source, &b.source))
			}
			(Self::Sink(a), Self::Sink(b)) => desc(&a.sink, &b.sink),
			(Self::NamespaceSink(a), Self::NamespaceSink(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.sink, &b.sink))
			}
			(Self::RowSettings(a), Self::RowSettings(b)) => storage_cmp(&a.storage, &b.storage),
			(Self::Procedure(a), Self::Procedure(b)) => desc(&a.procedure, &b.procedure),
			(Self::NamespaceProcedure(a), Self::NamespaceProcedure(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.procedure, &b.procedure))
			}
			(Self::ProcedureParam(a), Self::ProcedureParam(b)) => {
				desc(&a.procedure, &b.procedure).then_with(|| desc(&a.param_index, &b.param_index))
			}
			(Self::Binding(a), Self::Binding(b)) => desc(&a.binding, &b.binding),
			(Self::NamespaceBinding(a), Self::NamespaceBinding(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.binding, &b.binding))
			}
			(Self::OperatorSettings(a), Self::OperatorSettings(b)) => desc(&a.operator, &b.operator),
			(Self::ColumnSnapshot(a), Self::ColumnSnapshot(b)) => desc(&a.snapshot, &b.snapshot),
			(Self::SeriesColumnSnapshot(a), Self::SeriesColumnSnapshot(b)) => {
				desc(&a.series, &b.series).then_with(|| desc(&a.snapshot, &b.snapshot))
			}
			(Self::TableColumnSnapshot(a), Self::TableColumnSnapshot(b)) => {
				desc(&a.table, &b.table).then_with(|| desc(&a.snapshot, &b.snapshot))
			}
			(Self::VersionEpoch(a), Self::VersionEpoch(b)) => desc(&a.bucket, &b.bucket),
			(Self::IdentityAttribute(a), Self::IdentityAttribute(b)) => desc(&a.attribute, &b.attribute),
			(Self::IdentityAttributeValue(a), Self::IdentityAttributeValue(b)) => {
				desc(&a.identity, &b.identity).then_with(|| desc(&a.attribute, &b.attribute))
			}
			(Self::PartitionedRow(a), Self::PartitionedRow(b)) => storage_cmp(&a.storage, &b.storage)
				.then_with(|| desc(&a.partition, &b.partition))
				.then_with(|| desc(&a.row, &b.row)),
			(Self::Partition(a), Self::Partition(b)) => {
				object_cmp(&a.object, &b.object).then_with(|| desc(&a.partition, &b.partition))
			}
			(Self::Queue(a), Self::Queue(b)) => desc(&a.queue, &b.queue),
			(Self::NamespaceQueue(a), Self::NamespaceQueue(b)) => {
				desc(&a.namespace, &b.namespace).then_with(|| desc(&a.queue, &b.queue))
			}
			(Self::QueueDeduplication(a), Self::QueueDeduplication(b)) => {
				desc(&a.queue, &b.queue).then_with(|| desc(&a.tail, &b.tail))
			}
			(Self::Relationship(a), Self::Relationship(b)) => desc(&a.relationship, &b.relationship),
			(Self::SeriesRow(a), Self::SeriesRow(b)) => storage_cmp(&a.storage, &b.storage)
				.then_with(|| option_u8_cmp(&a.variant_tag, &b.variant_tag))
				.then_with(|| desc(&a.key, &b.key))
				.then_with(|| desc(&a.sequence, &b.sequence)),
			(Self::PartitionedSeriesRow(a), Self::PartitionedSeriesRow(b)) => {
				storage_cmp(&a.storage, &b.storage)
					.then_with(|| desc(&a.partition, &b.partition))
					.then_with(|| option_u8_cmp(&a.variant_tag, &b.variant_tag))
					.then_with(|| desc(&a.key, &b.key))
					.then_with(|| desc(&a.sequence, &b.sequence))
			}
			(Self::QueuePartition(a), Self::QueuePartition(b)) => {
				desc(&a.queue, &b.queue).then_with(|| desc(&a.partition, &b.partition))
			}
			(Self::QueueItemState(a), Self::QueueItemState(b)) => desc(&a.queue, &b.queue)
				.then_with(|| desc(&a.partition, &b.partition))
				.then_with(|| desc(&a.row, &b.row)),
			(Self::QueueDue(a), Self::QueueDue(b)) => desc(&a.queue, &b.queue)
				.then_with(|| desc(&a.partition, &b.partition))
				.then_with(|| desc(&a.due, &b.due))
				.then_with(|| desc(&a.row, &b.row)),
			(Self::QueueAttempt(a), Self::QueueAttempt(b)) => desc(&a.queue, &b.queue)
				.then_with(|| desc(&a.row, &b.row))
				.then_with(|| desc(&a.attempt, &b.attempt)),
			(Self::QueueKeyActive(a), Self::QueueKeyActive(b)) => desc(&a.queue, &b.queue)
				.then_with(|| desc(&a.partition, &b.partition))
				.then_with(|| desc(&a.key_hash, &b.key_hash))
				.then_with(|| desc(&a.row, &b.row)),
			(Self::SortedViewRow(a), Self::SortedViewRow(b)) => a.cmp(b),
			(Self::PartitionedSortedViewRow(a), Self::PartitionedSortedViewRow(b)) => a.cmp(b),
			(a, b) => desc(&(a.kind() as u8), &(b.kind() as u8)),
		}
	}
}

impl PartialOrd for AnyKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;

	use reifydb_codec::{key::encoded::EncodedKey, row::shape::fingerprint::RowShapeFingerprint};
	use reifydb_runtime::version_epoch::EpochSeconds;
	use reifydb_value::value::{
		Value, datetime::DateTime, dictionary::DictionaryId, identity::IdentityId, partition::Partition,
		row_number::RowNumber, sumtype::SumTypeId,
	};

	use super::{AnyKey, MetricCdcKey, MetricKey, MetricStorageKey};
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
			EncodableKey,
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
			sort_run::SortRun,
			system::{
				MigrationEventKey, MigrationKey, SystemSequenceKey, SystemVersion, SystemVersionKey,
				TransactionVersionKey, VersionEpochKey,
			},
			typed::key::Key,
		},
		value::index::encoded::EncodedIndexKey,
	};

	fn probe<K>(key: K) -> (AnyKey, EncodedKey)
	where
		K: Key + Clone,
		AnyKey: From<K>,
	{
		let encoded = Key::encode(&key);
		(AnyKey::from(key), encoded)
	}

	fn probe_encodable<K>(key: K) -> (AnyKey, EncodedKey)
	where
		K: EncodableKey + Clone,
		AnyKey: From<K>,
	{
		let encoded = EncodableKey::encode(&key);
		(AnyKey::from(key), encoded)
	}

	fn assert_ascending(probes: &[(AnyKey, EncodedKey)]) {
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

		assert_eq!(probes.first().unwrap().0.kind(), KeyKind::PartitionedSortedViewRow);
		assert_eq!(probes.last().unwrap().0.kind(), KeyKind::Namespace);
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

	fn probes() -> Vec<(AnyKey, EncodedKey)> {
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
	fn all_probes() -> Vec<(AnyKey, EncodedKey)> {
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

	#[test]
	fn test_every_variant_round_trips_through_the_shared_decoder() {
		// A key that comes back as the wrong variant rewrites the wrong row once Delta carries it.
		for (key, bytes) in all_probes() {
			assert_eq!(AnyKey::decode(&bytes), Some(key.clone()), "{key:?}");
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
		assert_eq!(AnyKey::decode(&Key::encode(&storage)), Some(AnyKey::Metric(MetricKey::Storage(storage))));
		assert_eq!(AnyKey::decode(&Key::encode(&cdc)), Some(AnyKey::Metric(MetricKey::Cdc(cdc))));
	}

	#[test]
	fn test_decode_rejects_bytes_that_name_no_live_kind() {
		// 0x24-0x26 and 0x3F were removed; a hole must not resurrect as a neighbouring key.
		assert_eq!(AnyKey::decode(&EncodedKey::new(Vec::<u8>::new())), None);
		for hole in [0x24u8, 0x25, 0x26, 0x3F] {
			assert_eq!(AnyKey::decode(&EncodedKey::new(vec![!hole, 0x00, 0x01])), None);
		}
	}

	#[test]
	fn test_decode_rejects_a_truncated_payload() {
		// A short read must fail rather than hand back a key built from whatever bytes arrived.
		let bytes = Key::encode(&RowKey {
			storage: StorageId::table(1),
			row: RowNumber(1),
		});
		let mut truncated = bytes.as_slice().to_vec();
		truncated.truncate(truncated.len() - 1);
		assert_eq!(AnyKey::decode(&EncodedKey::new(truncated)), None);
	}

	#[test]
	fn test_index_entry_with_an_empty_suffix_round_trips() {
		// set_encoded decodes what it is handed, so a write-only key would error on write.
		let key = IndexEntryKey::new(
			ObjectId::table(1),
			IndexId::primary(PrimaryKeyId(1)),
			EncodedIndexKey::new([0u8; 0]),
		);
		let bytes = EncodableKey::encode(&key);
		assert_eq!(AnyKey::from(key.clone()).encode(), bytes);
		assert_eq!(AnyKey::decode(&bytes), Some(AnyKey::from(key)));
	}

	#[test]
	fn test_any_key_fits_two_machine_words_beyond_an_encoded_key() {
		// Delta holds one key per pending write, so this number sets the per-transaction budget.
		assert_eq!(size_of::<EncodedKey>(), 48);
		assert_eq!(size_of::<AnyKey>(), 96);
		assert_eq!(size_of::<PartitionedSortedViewRowKey>(), size_of::<AnyKey>());
	}
}
