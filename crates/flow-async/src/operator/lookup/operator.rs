// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use reifydb_codec::row::{bytes::EncodedBytes, shape::RowShape};
use reifydb_core::{
	common::{CommitVersion, JoinType, SourceVersion},
	expression::Expression,
	flow::operator::LookupObject,
	interface::{
		catalog::{column::Column, flow::OperatorId, storage::StorageId},
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	internal_err,
	metrics::heap::OperatorSample,
	state::timer::TimerKind,
	value::{
		batch::{concat, empty_batch, from_encoded_bytes},
		column::factory::none,
	},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_flow::{context::FlowContext, error::FlowGraphError};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		column_view::ColumnView,
		datetime::DateTime,
		duration::Duration,
		row_number::RowNumber,
		system_columns::{require_row_numbers, resolve_column},
	},
};

use super::{
	expiry::LookupExpiry,
	partition::lookup_partition,
	store::{oldest_read, store_read, take_read},
};
use crate::{
	operator::{
		HostOperator, host::HostContext, join::column::JoinedColumnsBuilder, row_times,
		sink::decode_dictionary_columns, state::seal::ledger::FiredAt, state_access::mint_row_numbers,
	},
	timer::Timer,
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

pub struct LookupConfig {
	pub operator: OperatorId,
	pub left_node: OperatorId,
	pub join_type: JoinType,
	pub right: LookupObject,
	pub deferred: bool,
	pub storage: StorageId,
	pub columns: Vec<Column>,
	pub partition_by: Vec<String>,
	pub shape: RowShape,
	pub left: Vec<Expression>,
	pub left_schema: SchemaRef,
	pub right_schema: SchemaRef,
	pub alias: Option<String>,
	pub left_retention: Option<Duration>,
	pub routines: Routines,
	pub runtime_context: RuntimeContext,
	pub ctx: Arc<FlowContext>,
}

pub struct LookupOperator {
	config: LookupConfig,
	compiled_left: Vec<CompiledExpr>,
	expiry: LookupExpiry,
}

enum Published {
	Matched(RecordBatch),
	Unmatched(RecordBatch),
}

impl Published {
	fn kind(&self) -> usize {
		match self {
			Published::Matched(_) => 0,
			Published::Unmatched(_) => 1,
		}
	}

	fn into_batch(self) -> RecordBatch {
		match self {
			Published::Matched(batch) | Published::Unmatched(batch) => batch,
		}
	}
}

#[derive(Default)]
struct Output {
	removed: [Vec<RecordBatch>; 2],
	updated: [(Vec<RecordBatch>, Vec<RecordBatch>); 2],
	inserted: [Vec<RecordBatch>; 2],
	cleared: Vec<RowNumber>,
	armed: Vec<(RowNumber, DateTime)>,
}

impl Output {
	fn remove(&mut self, published: Published) {
		let kind = published.kind();
		self.removed[kind].push(published.into_batch());
	}

	fn insert(&mut self, published: Published) {
		let kind = published.kind();
		self.inserted[kind].push(published.into_batch());
	}

	fn update(&mut self, pre: Published, post: Published) {
		if pre.kind() != post.kind() {
			self.remove(pre);
			self.insert(post);
			return;
		}
		let kind = pre.kind();
		self.updated[kind].0.push(pre.into_batch());
		self.updated[kind].1.push(post.into_batch());
	}

	fn into_diffs(self, result: &mut Vec<Diff>) -> Result<()> {
		for batches in self.removed {
			if !batches.is_empty() {
				result.push(Diff::remove(concat(&batches)?));
			}
		}
		for (pre, post) in self.updated {
			if !pre.is_empty() {
				result.push(Diff::update(concat(&pre)?, concat(&post)?));
			}
		}
		for batches in self.inserted {
			if !batches.is_empty() {
				result.push(Diff::insert(concat(&batches)?));
			}
		}
		Ok(())
	}
}

impl LookupOperator {
	pub fn new(config: LookupConfig) -> Result<Self> {
		let compile_ctx = CompileContext {
			symbols: &config.ctx.symbols,
		};
		let compiled_left = config
			.left
			.iter()
			.map(|expression| compile_expression(&compile_ctx, expression))
			.collect::<Result<Vec<_>>>()?;
		let expiry = LookupExpiry::new(config.left_retention);
		Ok(Self {
			config,
			compiled_left,
			expiry,
		})
	}

	fn builder(&self, left: &Schema) -> JoinedColumnsBuilder {
		JoinedColumnsBuilder::new(left, &self.config.right_schema, &self.config.alias, false)
	}

	fn key_values(&self, columns: &RecordBatch) -> Result<Vec<Option<Vec<Value>>>> {
		let row_count = columns.num_rows();
		if row_count == 0 {
			return Ok(Vec::new());
		}
		let session = EvalContext {
			params: &self.config.ctx.params,
			symbols: &self.config.ctx.symbols,
			routines: &self.config.routines,
			runtime_context: &self.config.runtime_context,
			identity: self.config.ctx.identity,
			is_aggregate_context: false,
			batch: empty_batch(),
			row_count: 1,
			target: None,
			take: None,
		};
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut key_columns: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(self.compiled_left.len());
		for compiled in &self.compiled_left {
			let column = match compiled.access_column_name() {
				Some(name) => match resolve_column(columns, name) {
					Some(index) => (
						columns.schema_ref().fields()[index].clone(),
						columns.column(index).clone(),
					),
					None => none(name, row_count),
				},
				None => compiled.execute(&exec_ctx)?,
			};
			key_columns.push(column);
		}
		let views: Vec<ColumnView> = key_columns.iter().map(ColumnView::try_from).collect::<Result<_>>()?;

		let mut keys = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let values: Vec<Value> = views.iter().map(|view| view.get_value(row_idx)).collect();
			if values.iter().any(|value| matches!(value, Value::None { .. })) {
				keys.push(None);
			} else {
				keys.push(Some(values));
			}
		}
		Ok(keys)
	}

	fn read_version(&self, host: &dyn HostContext, source: SourceVersion) -> CommitVersion {
		match self.config.right {
			LookupObject::View(view) if self.config.deferred => host.lookup_view_version(view, source),
			LookupObject::Table(_) | LookupObject::View(_) => {
				let at = CommitVersion(source.0).min(host.version());
				host.lookup_floor().map_or(at, |floor| at.max(floor))
			}
		}
	}

	fn read_right(
		&self,
		host: &mut dyn HostContext,
		key: Option<&Vec<Value>>,
		version: CommitVersion,
	) -> Result<Option<(RowNumber, EncodedBytes)>> {
		let Some(values) = key else {
			return Ok(None);
		};
		let Some(partition) = lookup_partition(host, &self.config.columns, &self.config.partition_by, values)?
		else {
			return Ok(None);
		};
		host.lookup_read(self.config.storage, partition, version)
	}

	fn build(
		&self,
		host: &mut dyn HostContext,
		left: &RecordBatch,
		left_idx: usize,
		row_number: RowNumber,
		right: Option<(RowNumber, EncodedBytes)>,
	) -> Result<Published> {
		let builder = self.builder(left.schema_ref());
		match right {
			Some((right_number, bytes)) => {
				let mut right = from_encoded_bytes(&self.config.shape, &[right_number], &[bytes])?;
				decode_dictionary_columns(&mut right, host)?;
				Ok(Published::Matched(builder.join_cartesian(
					&[row_number],
					left,
					&[left_idx],
					&right,
					&[0],
				)?))
			}
			None => Ok(Published::Unmatched(builder.unmatched_left_batch(
				&[row_number],
				left,
				&[left_idx],
				&self.config.right_schema,
			)?)),
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn publish(
		&self,
		host: &mut dyn HostContext,
		post: &RecordBatch,
		left_idx: usize,
		key: Option<&Vec<Value>>,
		source: SourceVersion,
		reuse: Option<RowNumber>,
		output: &mut Output,
	) -> Result<Option<Published>> {
		if key.is_none() && self.config.join_type == JoinType::Inner {
			return Ok(None);
		}
		let version = self.read_version(host, source);
		let right = self.read_right(host, key, version)?;
		if right.is_none() && self.config.join_type == JoinType::Inner {
			return Ok(None);
		}
		let row_number = require_row_numbers(post)?[left_idx];
		let output_id = match reuse {
			Some(id) => id,
			None => mint_row_numbers(host, 1)?,
		};
		let published = self.build(host, post, left_idx, output_id, right)?;
		store_read(host, row_number, version, output_id)?;
		if let Some(at) = row_times(post)?.get(left_idx).copied().flatten() {
			output.armed.push((row_number, at));
		}
		Ok(Some(published))
	}

	fn undo(
		&self,
		host: &mut dyn HostContext,
		pre: &RecordBatch,
		left_idx: usize,
		key: Option<&Vec<Value>>,
		output: &mut Output,
	) -> Result<Option<(Published, RowNumber)>> {
		let row_number = require_row_numbers(pre)?[left_idx];
		let Some((version, output_id)) = take_read(host, row_number)? else {
			return Ok(None);
		};
		output.cleared.push(row_number);
		let right = self.read_right(host, key, version)?;
		if right.is_none() && self.config.join_type == JoinType::Inner {
			return internal_err!(
				"inner lookup published left row {} at version {} but the right row is gone at that version",
				row_number.value(),
				version.0
			);
		}
		Ok(Some((self.build(host, pre, left_idx, output_id, right)?, output_id)))
	}

	fn apply_insert(
		&mut self,
		host: &mut dyn HostContext,
		post: &RecordBatch,
		source: SourceVersion,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.key_values(post)?;
		let mut output = Output::default();
		for (left_idx, key) in keys.iter().enumerate() {
			if let Some(published) =
				self.publish(host, post, left_idx, key.as_ref(), source, None, &mut output)?
			{
				output.insert(published);
			}
		}
		self.finish(host, output, result)
	}

	fn apply_remove(
		&mut self,
		host: &mut dyn HostContext,
		pre: &RecordBatch,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.key_values(pre)?;
		let mut output = Output::default();
		for (left_idx, key) in keys.iter().enumerate() {
			if let Some((published, _)) = self.undo(host, pre, left_idx, key.as_ref(), &mut output)? {
				output.remove(published);
			}
		}
		self.finish(host, output, result)
	}

	fn apply_update(
		&mut self,
		host: &mut dyn HostContext,
		pre: &RecordBatch,
		post: &RecordBatch,
		source: SourceVersion,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let pre_keys = self.key_values(pre)?;
		let post_keys = self.key_values(post)?;
		let mut output = Output::default();
		for left_idx in 0..post.num_rows() {
			let undone = self.undo(
				host,
				pre,
				left_idx,
				pre_keys.get(left_idx).and_then(Option::as_ref),
				&mut output,
			)?;
			let published = self.publish(
				host,
				post,
				left_idx,
				post_keys.get(left_idx).and_then(Option::as_ref),
				source,
				undone.as_ref().map(|(_, id)| *id),
				&mut output,
			)?;
			match (undone.map(|(pre, _)| pre), published) {
				(Some(pre), Some(post)) => output.update(pre, post),
				(Some(pre), None) => output.remove(pre),
				(None, Some(post)) => output.insert(post),
				(None, None) => {}
			}
		}
		self.finish(host, output, result)
	}

	fn finish(&mut self, host: &mut dyn HostContext, mut output: Output, result: &mut Vec<Diff>) -> Result<()> {
		let cleared = mem::take(&mut output.cleared);
		let armed = mem::take(&mut output.armed);
		self.expiry.move_rows(host, &cleared, &armed)?;
		output.into_diffs(result)
	}
}

impl HostOperator for LookupOperator {
	fn id(&self) -> OperatorId {
		self.config.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		Some(OperatorSample::default())
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		if let ChangeOrigin::Flow(from_node) = &change.origin
			&& *from_node == self.config.operator
		{
			return Ok(Change::from_flow(
				self.config.operator,
				change.version,
				Vec::new(),
				DateTime::default(),
			));
		}

		let version = change.version;
		let source = version.source;
		let parent_origin = change.origin.clone();
		let mut result = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			let origin = diff.origin().cloned().unwrap_or_else(|| parent_origin.clone());
			if !matches!(origin, ChangeOrigin::Flow(from_node) if from_node == self.config.left_node) {
				return Err(Error::from(FlowGraphError::UnknownDiffOrigin {
					operator: "Lookup",
					origin: None,
				}));
			}
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_insert(host, &post, source, &mut result)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_remove(host, &pre, &mut result)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_update(host, &pre, &post, source, &mut result)?,
			}
		}

		Ok(Change::from_flow(self.config.operator, version, result, change.changed_at))
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		if timer.kind == TimerKind::Maintenance {
			self.expiry.free_due(host, FiredAt::of(&timer))?;
		}
		Ok(None)
	}

	fn seal_span(&self) -> Option<Duration> {
		self.expiry.retention()
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		Some(self.builder(&self.config.left_schema).schema(&self.config.left_schema, &self.config.right_schema))
	}

	fn oldest_read_version(&self, host: &mut dyn HostContext) -> Result<Option<CommitVersion>> {
		oldest_read(host)
	}
}

#[cfg(test)]
mod tests {
	use std::{ops::Bound, sync::Arc};

	use arrow_array::UInt64Array;
	use reifydb_codec::row::{bytes::read_fingerprint, shape::RowFamily};
	use reifydb_core::{
		actors::pending::PendingWrite,
		common::ChangeVersion,
		interface::{
			catalog::{
				column::ColumnIndex,
				dictionary::Dictionary,
				id::{ColumnId, ViewId},
				table::Table,
				view::{TableView, View, ViewKind},
			},
			resolved::{ResolvedNamespace, ResolvedView},
		},
		key::{any::TaggedKey, row::StoragePartitionedRowKey},
		row::row_shape_from_columns,
		value::{
			batch::batch,
			column::factory::{int4, utf8, utf8_with_bitvec},
		},
	};
	use reifydb_rql::expression::parse_expression;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::{
		factory::time::at_millis,
		fragment::Fragment,
		value::{
			constraint::{Constraint, TypeConstraint},
			container::temporal_array::datetime_array,
			identity::IdentityId,
			partition::Partition,
			system_columns::{SystemColumn, with_system_column},
			value_type::ValueType,
		},
	};

	use super::*;
	use crate::{
		operator::{
			host::TxnHostContext,
			lookup::store::stored_read,
			scan::catalog_schema,
			sink::{DurableSink, view::SinkTableViewOperator},
		},
		timer::extension::TimerExtension,
		transaction::{
			FlowTransaction, LookupContext, LookupVersions, deferred::DeferredTransaction, mock::FlowTxn,
			substrate::apply_operator_state,
		},
	};

	const OP: u64 = 77;
	const LEFT: u64 = 76;

	struct NoProducer;

	impl LookupVersions for NoProducer {
		fn view_version(&self, _view: ViewId, _source: SourceVersion) -> Option<CommitVersion> {
			None
		}

		fn view_commit_through(&self, _view: ViewId, _commit: CommitVersion) -> Option<CommitVersion> {
			None
		}
	}

	fn right_table(engine: &TestEngine, dictionary: bool) -> Table {
		engine.admin("CREATE NAMESPACE t");
		if dictionary {
			engine.admin("CREATE DICTIONARY t::d FOR utf8 AS uint4");
			engine.admin(
				"CREATE TABLE t::r { k: utf8 with { dictionary: t::d }, v: int4 } WITH { partition: { by: { k } } }",
			);
		} else {
			engine.admin("CREATE TABLE t::r { k: utf8, v: int4 } WITH { partition: { by: { k } } }");
		}
		let catalog = engine.inner().catalog();
		let namespace = catalog.cache().find_namespace_by_name("t").expect("namespace t");
		catalog.cache().find_table_by_name(namespace.id(), "r").expect("table t::r")
	}

	fn dictionary_of(engine: &TestEngine) -> Dictionary {
		let catalog = engine.inner().catalog();
		let namespace = catalog.cache().find_namespace_by_name("t").expect("namespace t");
		catalog.cache().find_dictionary_by_name(namespace.id(), "d").expect("dictionary t::d")
	}

	fn put_right(engine: &TestEngine, key: &str, value: i32) -> CommitVersion {
		engine.command(&format!("INSERT t::r [{{ k: '{key}', v: {value} }}]"));
		engine.inner().current_version().expect("current version")
	}

	fn left_schema() -> SchemaRef {
		batch(vec![utf8("lk", Vec::<String>::new())]).unwrap().schema()
	}

	fn lookup(table: &Table, join_type: JoinType, retention: Option<Duration>) -> LookupOperator {
		LookupOperator::new(LookupConfig {
			operator: OperatorId(OP),
			left_node: OperatorId(LEFT),
			join_type,
			right: LookupObject::Table(table.id),
			deferred: true,
			storage: StorageId::from(table.id),
			columns: table.columns.clone(),
			partition_by: table.partition_by.clone(),
			shape: row_shape_from_columns(RowFamily::Table, &table.columns),
			left: parse_expression("lk").expect("the left key parses"),
			left_schema: left_schema(),
			right_schema: catalog_schema(&table.columns),
			alias: Some("r".to_string()),
			left_retention: retention,
			routines: Routines::empty(),
			runtime_context: RuntimeContext::testing(0, 1),
			ctx: Arc::new(FlowContext::default()),
		})
		.expect("the lookup operator must build")
	}

	fn txn_at(engine: &TestEngine, version: CommitVersion) -> DeferredTransaction {
		engine.flow_txn().at(version).catalog(engine.inner().catalog()).deferred()
	}

	fn rows(keys: &[Option<&str>], numbers: &[u64], at: DateTime) -> RecordBatch {
		let values: Vec<String> = keys.iter().map(|key| key.unwrap_or_default().to_string()).collect();
		let valid: Vec<bool> = keys.iter().map(Option::is_some).collect();
		let columns = batch(vec![utf8_with_bitvec("lk", values, valid)]).unwrap();
		let columns = with_system_column(
			columns,
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(numbers.to_vec())),
		)
		.unwrap();
		with_system_column(columns, SystemColumn::Time, Arc::new(datetime_array(vec![at; keys.len()]))).unwrap()
	}

	fn run(op: &mut LookupOperator, txn: &mut DeferredTransaction, source: CommitVersion, diff: Diff) -> Vec<Diff> {
		let change = Change::from_flow(
			OperatorId(LEFT),
			ChangeVersion::from(source),
			vec![diff],
			DateTime::default(),
		);
		op.apply(&mut TxnHostContext::new(txn, OperatorId(OP)), change).unwrap().diffs.to_vec()
	}

	fn commit(engine: &TestEngine, txn: &mut DeferredTransaction) {
		// Operator state only reaches the store through the batch, so a later txn needs a commit first.
		apply_operator_state(&engine.inner().operator_state(), &txn.take_pending());
	}

	fn stored(txn: &mut DeferredTransaction, row: u64) -> Option<CommitVersion> {
		stored_read(&mut TxnHostContext::new(txn, OperatorId(OP)), RowNumber(row))
			.unwrap()
			.map(|(version, _)| version)
	}

	fn stored_output(txn: &mut DeferredTransaction, row: u64) -> RowNumber {
		let read = stored_read(&mut TxnHostContext::new(txn, OperatorId(OP)), RowNumber(row)).unwrap();
		read.unwrap_or_else(|| panic!("row {row} has no stored read")).1
	}

	fn oldest(op: &LookupOperator, txn: &mut DeferredTransaction) -> Option<CommitVersion> {
		op.oldest_read_version(&mut TxnHostContext::new(txn, OperatorId(OP))).unwrap()
	}

	fn cell(columns: &RecordBatch, name: &str, row: usize) -> Value {
		let index = columns.schema().index_of(name).unwrap_or_else(|_| panic!("output has no column {name}"));
		ColumnView::try_from((columns.column(index), columns.schema().field(index))).unwrap().get_value(row)
	}

	fn only_insert(diffs: &[Diff]) -> RecordBatch {
		assert_eq!(diffs.len(), 1, "exactly one diff expected, got {diffs:?}");
		match &diffs[0] {
			Diff::Insert {
				post,
				..
			} => post.clone(),
			other => panic!("expected an insert, got {other:?}"),
		}
	}

	fn only_remove(diffs: &[Diff]) -> RecordBatch {
		assert_eq!(diffs.len(), 1, "exactly one diff expected, got {diffs:?}");
		match &diffs[0] {
			Diff::Remove {
				pre,
				..
			} => pre.clone(),
			other => panic!("expected a remove, got {other:?}"),
		}
	}

	fn stored_partitions(engine: &TestEngine, storage: StorageId) -> Vec<(Partition, EncodedBytes)> {
		let query = engine.inner().multi().begin_query().unwrap();
		query.range_partitioned_row(storage, Bound::Unbounded, Bound::Unbounded, RangeScope::All, 16)
			.map(|row| {
				let row = row.unwrap();
				let key: StoragePartitionedRowKey = row.key;
				(key.partition.0, row.bytes)
			})
			.collect()
	}

	fn commit_flow_pending(engine: &TestEngine, txn: &mut DeferredTransaction) {
		let pending = txn.take_pending();
		let mut cmd = engine.begin_admin(IdentityId::system()).unwrap();
		for (key, write) in pending.iter_sorted() {
			let key = TaggedKey::decode(key).unwrap();
			match write {
				PendingWrite::Set(value) => cmd.set(&key, value.clone()).unwrap(),
				PendingWrite::Remove {
					..
				} => cmd.remove(&key).unwrap(),
			};
		}
		cmd.commit().unwrap();
	}

	#[test]
	#[allow(clippy::disallowed_methods)]
	fn lookup_partition_equals_the_partition_a_table_and_a_view_stored() {
		// S1: the lookup hashes the dictionary id, as both writers do; a plain-value hash would land elsewhere.
		let engine = TestEngine::new();
		let table = right_table(&engine, true);
		let version = put_right(&engine, "a", 1);
		let stored = stored_partitions(&engine, StorageId::from(table.id));
		assert_eq!(stored.len(), 1, "the table must hold the one inserted row");

		let mut txn = txn_at(&engine, version);
		let mut host = TxnHostContext::new(&mut txn, OperatorId(OP));
		let partition = lookup_partition(
			&mut host,
			&table.columns,
			&table.partition_by,
			&[Value::Utf8("a".to_string())],
		)
		.unwrap();
		assert_eq!(partition, Some(stored[0].0), "the table row must be found under the lookup's partition");
		assert_ne!(
			partition,
			Some(Partition::of(&[Value::Utf8("a".to_string())])),
			"a dictionary column must not hash its plain value"
		);
		// V3: the stored table row decodes with the shape the operator derives from the catalog columns.
		assert_eq!(
			read_fingerprint(&stored[0].1),
			row_shape_from_columns(RowFamily::Table, &table.columns).fingerprint(),
			"a table row written by the engine must carry the catalog-derived shape"
		);

		let dictionary = dictionary_of(&engine);
		let namespace = engine.inner().catalog().cache().find_namespace_by_name("t").unwrap();
		let view_columns = vec![
			Column {
				id: ColumnId(901),
				name: "k".to_string(),
				constraint: TypeConstraint::with_constraint(
					ValueType::Utf8,
					Constraint::Dictionary(dictionary.id, dictionary.id_type.clone()),
				),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: Some(dictionary.id),
			},
			Column {
				id: ColumnId(902),
				name: "v".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Int4),
				properties: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
				dictionary_id: None,
			},
		];
		let view = View::Table(TableView {
			id: ViewId(900),
			namespace: namespace.id(),
			name: "pv".to_string(),
			kind: ViewKind::Deferred,
			columns: view_columns.clone(),
			primary_key: None,
			partition_by: vec!["k".to_string()],
			sort: vec![],
		});
		let storage = view.storage_id();
		let mut sink = SinkTableViewOperator::new(
			OperatorId(OP + 1),
			ResolvedView::new(
				Fragment::internal("pv"),
				ResolvedNamespace::new(Fragment::internal("t"), namespace),
				view,
			),
			vec!["k".to_string()],
			RuntimeContext::testing(0, 1),
		);
		let stamp = || -> ArrayRef { Arc::new(datetime_array([DateTime::from_nanos(1_000)])) };
		let input = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64])) as ArrayRef),
			(SystemColumn::CreatedAt, stamp()),
			(SystemColumn::UpdatedAt, stamp()),
			(SystemColumn::Time, stamp()),
		]
		.into_iter()
		.fold(batch(vec![utf8("k", ["a"]), int4("v", [5])]).unwrap(), |columns, (column, array)| {
			with_system_column(columns, column, array).unwrap()
		});
		let mut sink_txn = txn_at(&engine, version);
		sink.apply(
			&mut sink_txn,
			Change::from_flow(
				OperatorId(LEFT),
				ChangeVersion::from(version),
				vec![Diff::insert(input)],
				DateTime::default(),
			),
		)
		.unwrap();
		commit_flow_pending(&engine, &mut sink_txn);

		let stored = stored_partitions(&engine, storage);
		assert_eq!(stored.len(), 1, "the view sink must have written the one row");
		let later = engine.inner().current_version().unwrap();
		let mut txn = txn_at(&engine, later);
		let mut host = TxnHostContext::new(&mut txn, OperatorId(OP));
		let partition =
			lookup_partition(&mut host, &view_columns, &["k".to_string()], &[Value::Utf8("a".to_string())])
				.unwrap();
		assert_eq!(partition, Some(stored[0].0), "the view row must be found under the lookup's partition");
		assert_eq!(
			read_fingerprint(&stored[0].1),
			row_shape_from_columns(RowFamily::Table, &view_columns).fingerprint(),
			"a view row written by the sink must carry the catalog-derived shape"
		);
	}

	#[test]
	fn an_inner_lookup_returns_the_right_row_of_its_partition() {
		// S2: one left row, one right row under the same key, one output row numbered like the left row.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		put_right(&engine, "b", 20);
		let version = put_right(&engine, "a", 10);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, version);

		let out = only_insert(&run(
			&mut op,
			&mut txn,
			version,
			Diff::insert(rows(&[Some("a")], &[7], at_millis(5))),
		));

		assert_eq!(out.num_rows(), 1);
		assert_eq!(cell(&out, "lk", 0), Value::Utf8("a".to_string()));
		assert_eq!(cell(&out, "r_k", 0), Value::Utf8("a".to_string()));
		assert_eq!(cell(&out, "r_v", 0), Value::Int4(10), "the row of partition a, not of b");
		assert_eq!(
			require_row_numbers(&out).unwrap(),
			&[stored_output(&mut txn, 7)],
			"IC4: the output carries the id minted for its left row"
		);
		assert_ne!(
			require_row_numbers(&out).unwrap(),
			&[RowNumber(7)],
			"the output id is minted, not the input's"
		);
		assert_eq!(stored(&mut txn, 7), Some(version), "MD28: the read version is stored per published row");
	}

	#[test]
	fn an_inner_lookup_with_no_match_emits_and_stores_nothing() {
		// LT1: an unmatched row must not leak, and must leave no read version that would pin the lease.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		let version = put_right(&engine, "a", 10);
		let mut op = lookup(&table, JoinType::Inner, Some(Duration::from_seconds(10).unwrap()));
		let mut txn = txn_at(&engine, version);

		let out =
			run(&mut op, &mut txn, version, Diff::insert(rows(&[Some("zz"), None], &[7, 8], at_millis(5))));

		assert!(out.is_empty(), "no output for an unmatched or none key, got {out:?}");
		assert_eq!(stored(&mut txn, 7), None);
		assert_eq!(stored(&mut txn, 8), None);
		assert_eq!(oldest(&op, &mut txn), None, "nothing stored, so nothing holds GC");
	}

	#[test]
	fn a_left_lookup_with_no_match_keeps_the_row_with_none_right_columns() {
		// LT2 and IC5: the left form publishes the row and stores its read version so the undo rebuilds it.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		let version = put_right(&engine, "a", 10);
		let mut op = lookup(&table, JoinType::Left, None);
		let mut txn = txn_at(&engine, version);

		let published = only_insert(&run(
			&mut op,
			&mut txn,
			version,
			Diff::insert(rows(&[Some("zz"), None], &[7, 8], at_millis(5))),
		));

		assert_eq!(published.num_rows(), 2, "both rows are kept");
		assert_eq!(cell(&published, "lk", 0), Value::Utf8("zz".to_string()));
		assert!(matches!(cell(&published, "r_v", 0), Value::None { .. }), "right columns are none");
		assert!(matches!(cell(&published, "r_k", 1), Value::None { .. }));
		assert_eq!(stored(&mut txn, 7), Some(version));
		assert_eq!(stored(&mut txn, 8), Some(version));

		let retracted = only_remove(&run(
			&mut op,
			&mut txn,
			version,
			Diff::remove(rows(&[Some("zz"), None], &[7, 8], at_millis(5))),
		));
		assert_eq!(retracted, published, "the retraction is exactly the published rows");
		assert_eq!(stored(&mut txn, 7), None, "the undo deletes the stored version");
	}

	#[test]
	fn a_retraction_after_the_right_row_changed_returns_the_published_row() {
		// The undo must re-read at the stored version, otherwise it retracts a row never sent.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		let first = put_right(&engine, "a", 10);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, first);
		let published = only_insert(&run(
			&mut op,
			&mut txn,
			first,
			Diff::insert(rows(&[Some("a")], &[7], at_millis(5))),
		));
		commit(&engine, &mut txn);
		assert_eq!(cell(&published, "r_v", 0), Value::Int4(10));

		let second = put_right(&engine, "a", 11);
		let mut txn = txn_at(&engine, second);
		let fresh = only_insert(&run(
			&mut op,
			&mut txn,
			second,
			Diff::insert(rows(&[Some("a")], &[8], at_millis(6))),
		));
		assert_eq!(cell(&fresh, "r_v", 0), Value::Int4(11), "a new left row sees the newest right row");

		let retracted = only_remove(&run(
			&mut op,
			&mut txn,
			second,
			Diff::remove(rows(&[Some("a")], &[7], at_millis(5))),
		));

		assert_eq!(retracted, published, "the retraction must equal the published row, not the newest");
		assert_eq!(stored(&mut txn, 7), None);
		assert_eq!(oldest(&op, &mut txn), Some(second), "only row 8's read remains");
	}

	#[test]
	fn an_update_retracts_the_old_pairing_and_publishes_the_new_one() {
		// S4.5: both sides published -> one update whose pre is the old right row and post the new key's row.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		put_right(&engine, "a", 10);
		let version = put_right(&engine, "b", 30);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, version);
		run(&mut op, &mut txn, version, Diff::insert(rows(&[Some("a")], &[7], at_millis(5))));

		let out = run(
			&mut op,
			&mut txn,
			version,
			Diff::update(rows(&[Some("a")], &[7], at_millis(5)), rows(&[Some("b")], &[7], at_millis(6))),
		);

		assert_eq!(out.len(), 1, "one update, got {out:?}");
		let Diff::Update {
			pre,
			post,
			..
		} = &out[0]
		else {
			panic!("expected an update, got {:?}", out[0]);
		};
		assert_eq!(cell(pre, "r_v", 0), Value::Int4(10));
		assert_eq!(cell(post, "r_v", 0), Value::Int4(30));

		let out = run(
			&mut op,
			&mut txn,
			version,
			Diff::update(rows(&[Some("b")], &[7], at_millis(6)), rows(&[Some("zz")], &[7], at_millis(7))),
		);
		let removed = only_remove(&out);
		assert_eq!(cell(&removed, "r_v", 0), Value::Int4(30), "an inner update to no match is a remove");
		assert_eq!(stored(&mut txn, 7), None);
	}

	#[test]
	fn an_in_place_update_keeps_the_minted_output_id() {
		// Pre and post of one update must share an output id, otherwise downstream sees a row move under an
		// update.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		put_right(&engine, "a", 10);
		let version = put_right(&engine, "b", 30);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, version);
		let published = only_insert(&run(
			&mut op,
			&mut txn,
			version,
			Diff::insert(rows(&[Some("a")], &[7], at_millis(5))),
		));
		let minted = require_row_numbers(&published).unwrap()[0];

		let out = run(
			&mut op,
			&mut txn,
			version,
			Diff::update(rows(&[Some("a")], &[7], at_millis(5)), rows(&[Some("b")], &[7], at_millis(6))),
		);

		let Diff::Update {
			pre,
			post,
			..
		} = &out[0]
		else {
			panic!("expected an update, got {out:?}");
		};
		assert_eq!(require_row_numbers(pre).unwrap(), &[minted], "the pre is the published row");
		assert_eq!(
			require_row_numbers(post).unwrap(),
			&[minted],
			"the post reuses the pre's id, no fresh mint"
		);
		assert_eq!(stored_output(&mut txn, 7), minted, "the stored id survives the update");
	}

	#[test]
	fn a_dictionary_miss_is_no_match_and_the_dictionary_does_not_grow() {
		// LT6 at operator level: the lookup only finds ids, it never interns the left value.
		let engine = TestEngine::new();
		let table = right_table(&engine, true);
		let version = put_right(&engine, "a", 10);
		let dictionary = dictionary_of(&engine);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, version);

		let matched = only_insert(&run(
			&mut op,
			&mut txn,
			version,
			Diff::insert(rows(&[Some("a")], &[6], at_millis(5))),
		));
		assert_eq!(cell(&matched, "r_k", 0), Value::Utf8("a".to_string()), "dictionary ids decode back");

		let out = run(&mut op, &mut txn, version, Diff::insert(rows(&[Some("nope")], &[7], at_millis(5))));
		assert!(out.is_empty(), "a value missing from the dictionary matches nothing");
		let mut host = TxnHostContext::new(&mut txn, OperatorId(OP));
		assert_eq!(
			host.dictionary_find(dictionary.id, &Value::Utf8("nope".to_string())).unwrap(),
			None,
			"the miss must not have interned the value"
		);
	}

	#[test]
	fn oldest_read_version_is_the_lowest_live_read() {
		// MD30 input: the lease must sit at the oldest read any live left row can still need.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		let first = put_right(&engine, "a", 10);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, first);
		run(&mut op, &mut txn, first, Diff::insert(rows(&[Some("a")], &[7], at_millis(5))));
		commit(&engine, &mut txn);
		let second = put_right(&engine, "a", 11);
		let mut txn2 = txn_at(&engine, second);
		run(&mut op, &mut txn2, second, Diff::insert(rows(&[Some("a")], &[8], at_millis(6))));

		assert_eq!(oldest(&op, &mut txn2), Some(first));
		run(&mut op, &mut txn2, second, Diff::remove(rows(&[Some("a")], &[7], at_millis(5))));
		assert_eq!(oldest(&op, &mut txn2), Some(second), "the floor moves up once the oldest row is undone");
	}

	#[test]
	#[should_panic(expected = "below the held lease floor")]
	fn an_undo_below_the_held_lease_panics() {
		// LT10 (MD31, IP1): a read version below the lease may be collected, so reading it must not be silent.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		let first = put_right(&engine, "a", 10);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, first);
		run(&mut op, &mut txn, first, Diff::insert(rows(&[Some("a")], &[7], at_millis(5))));
		commit(&engine, &mut txn);
		let second = put_right(&engine, "a", 11);

		let mut txn = txn_at(&engine, second);
		txn.lookup = Some(LookupContext {
			versions: Arc::new(NoProducer),
			floor: second,
		});
		run(&mut op, &mut txn, second, Diff::remove(rows(&[Some("a")], &[7], at_millis(5))));
	}

	#[test]
	fn an_expired_left_row_frees_its_read_version() {
		// LT11 at operator level: without the expiry the stored version pins the lease forever (MR9).
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		let version = put_right(&engine, "a", 10);
		let mut op = lookup(&table, JoinType::Inner, Some(Duration::from_seconds(10).unwrap()));
		let mut txn = txn_at(&engine, version);
		run(&mut op, &mut txn, version, Diff::insert(rows(&[Some("a")], &[7], at_millis(5_000))));
		run(&mut op, &mut txn, version, Diff::insert(rows(&[Some("a")], &[8], at_millis(30_000))));
		assert_eq!(oldest(&op, &mut txn), Some(version));

		let timer = Timer {
			due: at_millis(20_000),
			kind: TimerKind::Maintenance,
			key: LookupExpiry::timer_key(),
		};
		txn.disarm_timer(OperatorId(OP), &timer).unwrap();
		let emitted = op.on_timer(&mut TxnHostContext::new(&mut txn, OperatorId(OP)), timer).unwrap();

		assert!(emitted.is_none(), "an expiry emits nothing");
		assert_eq!(stored(&mut txn, 7), None, "row 7 is past event time + retention and is freed");
		assert_eq!(stored(&mut txn, 8), Some(version), "row 8 outlives the fire");
		let out = run(&mut op, &mut txn, version, Diff::remove(rows(&[Some("a")], &[7], at_millis(5_000))));
		assert!(out.is_empty(), "an expired row has nothing left to retract");
	}

	#[test]
	fn a_table_lookup_reads_at_the_left_rows_source_not_at_the_step_version() {
		// LT3 table form: a step that lags past a later right write must still pair the row with its source.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		let first = put_right(&engine, "a", 10);
		let second = put_right(&engine, "a", 11);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, second);

		let out = only_insert(&run(
			&mut op,
			&mut txn,
			first,
			Diff::insert(rows(&[Some("a")], &[7], at_millis(5))),
		));

		assert_eq!(cell(&out, "r_v", 0), Value::Int4(10), "reading at the step version would give 11");
		assert_eq!(stored(&mut txn, 7), Some(first), "the undo must re-read at the source it published at");
	}

	#[test]
	fn a_table_lookup_below_the_floor_reads_at_the_floor() {
		// A source under the lease may be collected, so the read is lifted to the floor instead of panicking.
		let engine = TestEngine::new();
		let table = right_table(&engine, false);
		let first = put_right(&engine, "a", 10);
		let second = put_right(&engine, "a", 11);
		let mut op = lookup(&table, JoinType::Inner, None);
		let mut txn = txn_at(&engine, second);
		txn.lookup = Some(LookupContext {
			versions: Arc::new(NoProducer),
			floor: second,
		});

		let out = only_insert(&run(
			&mut op,
			&mut txn,
			first,
			Diff::insert(rows(&[Some("a")], &[7], at_millis(5))),
		));

		assert_eq!(cell(&out, "r_v", 0), Value::Int4(11));
		assert_eq!(stored(&mut txn, 7), Some(second));
	}

	#[test]
	#[should_panic(expected = "sits below the held lease floor")]
	fn a_view_read_for_a_source_below_the_floor_panics() {
		// IP3: when the floor is above the source, no read at the floor can match what the source saw.
		let engine = TestEngine::new();
		let mut txn = txn_at(&engine, CommitVersion(10));
		txn.lookup = Some(LookupContext {
			versions: Arc::new(NoProducer),
			floor: CommitVersion(9),
		});
		let host = TxnHostContext::new(&mut txn, OperatorId(OP));
		host.lookup_view_version(ViewId(1), SourceVersion(8));
	}

	#[test]
	#[should_panic(expected = "found no completion of its producer")]
	fn a_view_read_with_no_producer_completion_panics() {
		// IP2: a version missing from the tracker history must not fall back to some other version.
		let engine = TestEngine::new();
		let mut txn = txn_at(&engine, CommitVersion(10));
		txn.lookup = Some(LookupContext {
			versions: Arc::new(NoProducer),
			floor: CommitVersion(3),
		});
		let host = TxnHostContext::new(&mut txn, OperatorId(OP));
		host.lookup_view_version(ViewId(1), SourceVersion(8));
	}

	struct Producer {
		for_source: CommitVersion,
		through_floor: CommitVersion,
	}

	impl LookupVersions for Producer {
		fn view_version(&self, _view: ViewId, _source: SourceVersion) -> Option<CommitVersion> {
			Some(self.for_source)
		}

		fn view_commit_through(&self, _view: ViewId, _commit: CommitVersion) -> Option<CommitVersion> {
			Some(self.through_floor)
		}
	}

	#[test]
	fn a_view_read_below_the_floor_reads_at_the_floor_when_the_producer_has_not_committed_since() {
		// With no producer commit between the source's commit and the floor, the floor read is exact and must
		// never panic.
		let engine = TestEngine::new();
		let mut txn = txn_at(&engine, CommitVersion(40));
		txn.lookup = Some(LookupContext {
			versions: Arc::new(Producer {
				for_source: CommitVersion(20),
				through_floor: CommitVersion(20),
			}),
			floor: CommitVersion(32),
		});
		let host = TxnHostContext::new(&mut txn, OperatorId(OP));

		assert_eq!(host.lookup_view_version(ViewId(1), SourceVersion(31)), CommitVersion(32));
	}

	#[test]
	#[should_panic(expected = "sits below the held lease floor")]
	fn a_view_read_below_the_floor_panics_when_the_producer_committed_since() {
		// A producer commit between the source's commit and the floor changed the rows the floor shows.
		let engine = TestEngine::new();
		let mut txn = txn_at(&engine, CommitVersion(40));
		txn.lookup = Some(LookupContext {
			versions: Arc::new(Producer {
				for_source: CommitVersion(20),
				through_floor: CommitVersion(25),
			}),
			floor: CommitVersion(32),
		});
		let host = TxnHostContext::new(&mut txn, OperatorId(OP));
		host.lookup_view_version(ViewId(1), SourceVersion(31));
	}
}
