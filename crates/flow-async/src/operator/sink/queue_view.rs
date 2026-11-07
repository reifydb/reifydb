// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, slice};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::row::{bytes::EncodedBytes, queue::EncodedQueueRow, shape::RowShape};
use reifydb_core::{
	actors::pending::PendingQueueItem,
	interface::{
		catalog::{
			column::Column,
			flow::OperatorId,
			queue::Queue,
			storage::StorageId,
			view::{QueueView, View},
		},
		change::{Change, Diff},
		flow::OperatorCapability,
		resolved::ResolvedView,
	},
	key::operator::{
		keyspace::ringbuffer::{RingbufferNextRow, RingbufferNextRowKey},
		state::{GroupId, GroupStateKey},
	},
	partition::{ordered_by_index, partition_by_indices, placement_of},
	state::typed::typed_key,
	value::column::factory,
};
use reifydb_flow::{
	operator::{
		sink::{
			SourceRowEncoder, coerce_columns,
			partition::{partition_batch, resolve_partitions_flow},
			shape_field_columns,
			table::dictionary_encode_view_columns,
		},
		source::decode_change_keeping,
		with_system_columns_of,
	},
	txn::Emit,
	writer::queue::QueueWriter,
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::{Value, partition::Partition, row_number::RowNumber, system_columns::user_columns},
};
use tracing::instrument;

use super::{
	DurableSink,
	ringbuffer_view::{decode_u64, encode_u64},
};
use crate::transaction::{deferred::DeferredTransaction, state::StateExtension};

const NEXT_ROW_STATE: &str = "QueueViewNextRow";

pub struct SinkQueueViewOperator {
	operator: OperatorId,
	view: ResolvedView,
	queue: Queue,
	shape: RowShape,
	storage: StorageId,
	user_columns: Vec<Column>,
	partition_indices: Vec<usize>,
	ordered_by: Option<usize>,
	verified_partitions: HashMap<Partition, Vec<Value>>,
	runtime_context: RuntimeContext,
}

impl SinkQueueViewOperator {
	pub fn new(
		operator: OperatorId,
		view: ResolvedView,
		queue: Queue,
		shape: RowShape,
		runtime_context: RuntimeContext,
	) -> Result<Self> {
		let user_columns =
			view.def().columns().iter().filter(|c| c.name != QueueView::CHANGE_COLUMN).cloned().collect();
		Ok(Self {
			operator,
			storage: view.def().storage_id(),
			view,
			partition_indices: partition_by_indices(&queue),
			ordered_by: ordered_by_index(&queue)?,
			queue,
			shape,
			user_columns,
			verified_partitions: HashMap::new(),
			runtime_context,
		})
	}

	fn next_row_key(&self) -> GroupStateKey {
		typed_key::<RingbufferNextRow>(GroupId::ROOT, &RingbufferNextRowKey {})
	}

	fn reserve_rows(&self, txn: &mut DeferredTransaction, count: u64) -> Result<u64> {
		let key = self.next_row_key();
		let base = match txn.state_get(self.operator, slice::from_ref(&key))?.pop().flatten() {
			Some(row) => decode_u64(&row, NEXT_ROW_STATE)?,
			None => 1,
		};
		txn.state_set(self.operator, vec![(key, encode_u64(base + count, NEXT_ROW_STATE)?)])?;
		Ok(base)
	}

	fn with_change(&self, columns: &RecordBatch, change: &str) -> Result<RecordBatch> {
		let mut user: Vec<(FieldRef, ArrayRef)> =
			user_columns(columns).map(|(field, array)| (field.clone(), array.clone())).collect();
		user.push(factory::utf8_repeated(QueueView::CHANGE_COLUMN, change, columns.num_rows()));
		with_system_columns_of(user, columns)
	}

	#[instrument(name = "flow::operator::sink::queue::enqueue", level = "trace", skip_all, fields(rows = rows.num_rows()))]
	fn enqueue(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		rows: &RecordBatch,
		change: &str,
	) -> Result<()> {
		if rows.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(rows, &self.user_columns, &self.runtime_context)?;
		let items = self.with_change(&coerced, change)?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &items)?;
		let source = dict_encoded.as_ref().unwrap_or(&items);
		let field_columns = shape_field_columns(source, &self.shape);
		let encoded = SourceRowEncoder::new(source, &self.shape, &field_columns)?.encode_all()?;
		if !self.partition_indices.is_empty() {
			let partitions = partition_batch(view, &self.partition_indices, source)?;
			resolve_partitions_flow(
				txn,
				self.storage.into(),
				&partitions.distinct,
				&mut self.verified_partitions,
			)?;
		}
		let base = self.reserve_rows(txn, encoded.len() as u64)?;
		let numbered: Vec<(RowNumber, EncodedBytes)> = encoded
			.into_iter()
			.enumerate()
			.map(|(index, row)| (RowNumber(base + index as u64), row))
			.collect();
		let admitted: Vec<PendingQueueItem> = numbered
			.iter()
			.map(|(row_number, row)| {
				let placement = placement_of(
					&self.shape,
					EncodedQueueRow::view(row),
					&self.partition_indices,
					self.ordered_by,
				);
				PendingQueueItem {
					queue: self.queue.id,
					partition: placement.partition,
					key_hash: placement.key_hash,
					row_number: *row_number,
					encoded: row.clone(),
				}
			})
			.collect();
		QueueWriter::new(txn, self.storage).insert(numbered)?;
		txn.pending.admit_queue_items(admitted);
		txn.emit(view.id(), Diff::insert(items))
	}
}

impl DurableSink for SinkQueueViewOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, txn: &mut DeferredTransaction, change: Change) -> Result<Change> {
		let change = decode_change_keeping(txn, change, &self.user_columns)?;
		let resolved = self.view.clone();
		let view = resolved.def();
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.enqueue(txn, view, post, QueueView::CHANGE_INSERT)?,
				Diff::Update {
					post,
					..
				} => self.enqueue(txn, view, post, QueueView::CHANGE_UPDATE)?,
				Diff::Remove {
					pre,
					..
				} => self.enqueue(txn, view, pre, QueueView::CHANGE_REMOVE)?,
			}
		}
		Ok(Change::from_flow(self.operator, change.version, Vec::new(), change.changed_at))
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::UInt64Array;
	use reifydb_codec::row::shape::RowFamily;
	use reifydb_core::{
		actors::pending::PendingWrite,
		common::{ChangeVersion, CommitVersion, TimeSource},
		interface::{
			catalog::{
				column::{Column, ColumnIndex},
				id::{ColumnId, NamespaceId, QueueId, ViewId},
				namespace::Namespace,
				queue::{QueueDispatch, QueueRetention, QueueRetry},
				view::{QueueView, ViewKind},
			},
			resolved::ResolvedNamespace,
		},
		key::{any::TaggedKey, row::RowKey},
		partition::QueuePlacement,
		row::row_shape_from_columns,
		value::{batch::batch, column::factory::int4_optional},
	};
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		spawner::Spawner,
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		fragment::Fragment,
		value::{
			constraint::TypeConstraint,
			container::temporal_array::datetime_array,
			datetime::DateTime,
			system_columns::{SystemColumn, with_system_column},
			value_type::ValueType,
		},
	};

	use super::*;
	use crate::transaction::mock::FlowTxn;

	const QUEUE: QueueId = QueueId(9);

	fn column(index: u8, name: &str, ty: ValueType) -> Column {
		Column {
			id: ColumnId(index as u64 + 1),
			name: name.to_string(),
			constraint: TypeConstraint::unconstrained(ty),
			properties: vec![],
			index: ColumnIndex(index),
			auto_increment: false,
			dictionary_id: None,
		}
	}

	#[derive(Default)]
	struct Options {
		partition_by: Vec<String>,
		ordered_by: Option<String>,
	}

	fn sink(options: Options) -> SinkQueueViewOperator {
		let columns = vec![
			column(0, "id", ValueType::Int4),
			column(1, "tenant", ValueType::Utf8),
			column(2, QueueView::CHANGE_COLUMN, ValueType::Utf8),
		];
		let def = View::Queue(QueueView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "q".to_string(),
			kind: ViewKind::Deferred,
			columns: columns.clone(),
			primary_key: None,
			partition_by: options.partition_by.clone(),
			queue: QUEUE,
			sort: vec![],
		});
		let queue = Queue {
			id: QUEUE,
			namespace: NamespaceId(1),
			name: "q".to_string(),
			columns: columns.clone(),
			partition_by: options.partition_by,
			dispatch: QueueDispatch::Fifo {
				ordered_by: options.ordered_by,
			},
			deduplicate: None,
			retention: QueueRetention::default(),
			retry: QueueRetry::default(),
			time: TimeSource::None,
		};
		let namespace = Namespace::Local {
			id: NamespaceId(1),
			name: "n".to_string(),
			local_name: "n".to_string(),
			parent_id: NamespaceId::ROOT,
		};
		let resolved = ResolvedView::new(
			Fragment::internal("q"),
			ResolvedNamespace::new(Fragment::internal("n"), namespace),
			def,
		);
		let shape = row_shape_from_columns(RowFamily::Queue, &columns);
		SinkQueueViewOperator::new(
			OperatorId(1),
			resolved,
			queue,
			shape,
			RuntimeContext::builder(Spawner::inline())
				.clock(Clock::Mock(MockClock::from_millis(0)))
				.build(),
		)
		.unwrap()
	}

	fn rows(cells: &[(i32, &str)]) -> RecordBatch {
		let user = batch(vec![
			int4_optional("id", cells.iter().map(|(id, _)| Some(*id))),
			factory::utf8("tenant", cells.iter().map(|(_, tenant)| tenant.to_string())),
		])
		.unwrap();
		let stamp =
			|| -> ArrayRef { Arc::new(datetime_array(cells.iter().map(|_| DateTime::from_nanos(1_000)))) };
		let numbers: Vec<u64> = (0..cells.len()).map(|n| 100 + n as u64).collect();
		let system: [(SystemColumn, ArrayRef); 4] = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(numbers))),
			(SystemColumn::CreatedAt, stamp()),
			(SystemColumn::UpdatedAt, stamp()),
			(SystemColumn::Time, stamp()),
		];
		system.into_iter()
			.fold(user, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	fn change(diffs: Vec<Diff>) -> Change {
		Change::from_flow(OperatorId(1), ChangeVersion::from(CommitVersion(1)), diffs, DateTime::from_nanos(0))
	}

	fn stored(sink: &SinkQueueViewOperator, txn: &DeferredTransaction) -> Vec<(u64, i32, String, String)> {
		txn.pending
			.rows_ordered()
			.filter_map(|(key, write)| {
				let TaggedKey::Row(key) = TaggedKey::decode(key)? else {
					return None;
				};
				assert_eq!(
					key.storage,
					StorageId::Queue(QUEUE),
					"an item row must live under the queue"
				);
				let PendingWrite::Set(bytes) = write else {
					panic!("a queue view must never remove a row");
				};
				let cell = |index| sink.shape.get_value(bytes.as_slice(), index);
				let (Value::Int4(id), Value::Utf8(tenant), Value::Utf8(label)) =
					(cell(0), cell(1), cell(2))
				else {
					panic!("an item row must hold its typed cells");
				};
				Some((key.row.0, id, tenant, label))
			})
			.collect()
	}

	#[allow(clippy::disallowed_methods)]
	fn hash(value: &str) -> Partition {
		Partition::of(&[Value::Utf8(value.to_string())])
	}

	#[test]
	fn every_change_kind_appends_exactly_one_item_with_its_label_and_the_right_row() {
		// An update replacing its item, or a remove carrying the post row, would break the outbox contract.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let mut sink = sink(Options::default());
		sink.apply(
			&mut txn,
			change(vec![
				Diff::insert(rows(&[(1, "a")])),
				Diff::update(rows(&[(1, "a")]), rows(&[(1, "b")])),
				Diff::remove(rows(&[(1, "b")])),
			]),
		)
		.unwrap();
		assert_eq!(
			stored(&sink, &txn),
			vec![
				(1, 1, "a".to_string(), "insert".to_string()),
				(2, 1, "b".to_string(), "update".to_string()),
				(3, 1, "b".to_string(), "remove".to_string()),
			]
		);
		assert_eq!(txn.pending.queue_items().len(), 3, "every written item must also be admitted");
	}

	#[test]
	fn a_change_batch_keeps_its_row_order_and_row_numbers_keep_counting() {
		// FIFO order is row number order, so reuse or a reordered batch would hand workers changes out of
		// order.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let mut sink = sink(Options::default());
		sink.apply(&mut txn, change(vec![Diff::insert(rows(&[(7, "a"), (8, "a")]))])).unwrap();
		sink.apply(&mut txn, change(vec![Diff::insert(rows(&[(9, "a")]))])).unwrap();
		let order: Vec<(u64, i32)> = stored(&sink, &txn).into_iter().map(|(row, id, ..)| (row, id)).collect();
		assert_eq!(order, vec![(1, 7), (2, 8), (3, 9)]);
		let admitted: Vec<u64> = txn.pending.queue_items().iter().map(|item| item.row_number.0).collect();
		assert_eq!(admitted, vec![1, 2, 3], "scheduling must name the same rows the writer stored");
	}

	#[test]
	fn an_unpartitioned_unordered_item_is_admitted_to_the_one_fifo_with_no_key() {
		// Any other partition spreads a one-FIFO queue away from where claim looks; a key would block siblings.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let mut sink = sink(Options::default());
		sink.apply(&mut txn, change(vec![Diff::insert(rows(&[(1, "a")]))])).unwrap();
		let item = &txn.pending.queue_items()[0];
		assert_eq!(item.queue, QUEUE);
		assert_eq!(item.partition, Queue::UNPARTITIONED);
		assert_eq!(item.key_hash, None);
	}

	#[test]
	fn the_placement_comes_from_the_row_in_the_item_so_a_moved_value_lands_in_the_new_partition() {
		// Placing by the pre row would strand the item where a restart's hydration never finds it.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let mut sink = sink(Options {
			partition_by: vec!["tenant".to_string()],
			ordered_by: Some("tenant".to_string()),
			..Options::default()
		});
		sink.apply(
			&mut txn,
			change(vec![
				Diff::insert(rows(&[(1, "a")])),
				Diff::update(rows(&[(1, "a")]), rows(&[(1, "b")])),
			]),
		)
		.unwrap();
		let items = txn.pending.queue_items();
		assert_eq!(items[0].partition, hash("a"));
		assert_eq!(items[1].partition, hash("b"));
		assert_eq!(items[1].key_hash, Some(hash("b").0 as u64), "the key must follow the post row too");
		for item in items {
			assert_eq!(
				placement_of(
					&sink.shape,
					EncodedQueueRow::view(&item.encoded),
					&sink.partition_indices,
					sink.ordered_by
				),
				QueuePlacement {
					partition: item.partition,
					key_hash: item.key_hash,
				},
				"admission must match what hydration recomputes from the stored row"
			);
		}
	}

	#[test]
	fn an_item_row_lives_under_the_queue_and_never_under_the_view() {
		// Rows under the view id would be invisible to claim, which reads only the queue's row keys.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();
		let mut sink = sink(Options::default());
		sink.apply(&mut txn, change(vec![Diff::insert(rows(&[(1, "a")]))])).unwrap();
		assert!(txn.pending.write_at(&RowKey::new(QUEUE, RowNumber(1)).encode()).is_some());
		assert!(txn
			.pending
			.write_at(&RowKey::new(StorageId::View(ViewId(1)), RowNumber(1)).encode())
			.is_none());
	}
}
