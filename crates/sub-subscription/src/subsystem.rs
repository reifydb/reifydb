// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	collections::HashMap,
	result::Result as StdResult,
	sync::{Arc, RwLock},
	time::Duration,
};

use dashmap::DashMap;
use reifydb_catalog::catalog::Catalog;
use reifydb_cdc::{
	consume::{
		consumer::CdcConsumer,
		poll::{PollConsumer, PollConsumerConfig},
		watermark::CdcConsumerWatermark,
	},
	storage::CdcStore,
};
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	error::diagnostic::catalog::subscription_not_found,
	interface::{
		WithEventBus,
		catalog::{
			flow::{FlowId, FlowNodeId},
			id::SubscriptionId,
			shape::ShapeId,
			subscription::{SubscriptionInspector, SubscriptionInspectorRef},
		},
		cdc::CdcConsumerId,
		change::{Change, Diff},
		version::{ComponentType, HasVersion, SystemVersion},
	},
	metric::{ExecutionMetrics, StatementMetric},
	util::ioc::IocContainer,
	value::column::columns::Columns,
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, HydrateOutcome, SubscriptionService, SubscriptionServiceRef},
};
use reifydb_rql::{
	expression::{ColumnExpression, ConstantExpression, Expression},
	fingerprint::request::fingerprint_request,
	flow::{flow::FlowDag, node::FlowNodeType},
};
use reifydb_runtime::{SharedRuntime, context::RuntimeContext};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem, SubsystemFactory};
use reifydb_sub_flow::{
	builder::OperatorFactory, engine::FlowEngine, operator::Operators, transaction::FlowTransaction,
};
use reifydb_transaction::{
	interceptor::builder::InterceptorBuilder,
	multi::{lease::VersionLeaseGuard, transaction::MultiTransaction},
	transaction::{Transaction, query::QueryTransaction},
};
use reifydb_type::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{datetime::DateTime, duration::Duration as ReifyDuration, identity::IdentityId, row_number::RowNumber},
};

use crate::{
	consumer::SubscriptionCdcConsumer,
	sink::{DeliveryBuffer, EphemeralSinkSubscriptionOperator},
	store::SubscriptionStore,
};

struct SubscriptionState {
	store: Arc<SubscriptionStore>,
	flow_engine: Arc<RwLock<FlowEngine>>,
	flow_states: Arc<DashMap<FlowId, HashMap<EncodedKey, EncodedRow>>>,

	hydration_versions: Arc<DashMap<FlowId, CommitVersion>>,

	subscription_flows: RwLock<HashMap<SubscriptionId, FlowId>>,

	delivery: Arc<DeliveryBuffer>,

	multi: MultiTransaction,
	catalog: Catalog,
}

struct SubscriptionServiceImpl {
	state: Arc<SubscriptionState>,
}

impl SubscriptionService for SubscriptionServiceImpl {
	fn register_subscription(
		&self,
		id: SubscriptionId,
		flow_dag: FlowDag,
		column_names: Vec<String>,
		txn: &mut Transaction<'_>,
	) -> Result<()> {
		self.state.store.register(id, column_names);

		let flow_id = flow_dag.id;
		{
			let mut engine = self.state.flow_engine.write().unwrap();
			register_ephemeral_flow(&mut engine, txn, flow_dag, id, self.state.delivery.clone())?;
		}

		self.state.subscription_flows.write().unwrap().insert(id, flow_id);

		self.state.flow_states.insert(flow_id, HashMap::new());

		Ok(())
	}

	fn unregister_subscription(&self, id: &SubscriptionId) -> Result<()> {
		let existed = self.state.store.unregister(id);

		if let Some(flow_id) = self.state.subscription_flows.write().unwrap().remove(id) {
			self.state.flow_states.remove(&flow_id);
			self.state.hydration_versions.remove(&flow_id);

			self.state.flow_engine.write().unwrap().remove_flow(flow_id);
		}

		if existed {
			Ok(())
		} else {
			Err(Error(Box::new(subscription_not_found(
				Fragment::internal(format!("subscription_{}", id.0)),
				&format!("subscription_{}", id.0),
			))))
		}
	}

	fn next_id(&self) -> SubscriptionId {
		self.state.store.next_id()
	}

	fn hydrate(
		&self,
		sub_id: SubscriptionId,
		engine: &StandardEngine,
		identity: IdentityId,
		lease: VersionLeaseGuard,
		max_rows: u64,
	) -> StdResult<HydrateOutcome, HydrateError> {
		let flow_id = self
			.state
			.subscription_flows
			.read()
			.unwrap()
			.get(&sub_id)
			.copied()
			.ok_or(HydrateError::SubscriptionNotFound)?;

		let version = lease.version();
		self.state.hydration_versions.insert(flow_id, version);
		let hydrate_start = engine.clock().instant();

		let mut outer = engine.begin_query_at_version(&lease, identity)?;

		let sources =
			collect_source_descriptors(&self.state.flow_engine, flow_id, &self.state.catalog, &mut outer)?;

		let mut total_rows: u64 = 0;
		let mut source_frames: Vec<(ShapeId, Vec<Columns>)> = Vec::with_capacity(sources.len());
		let mut statements: Vec<StatementMetric> = Vec::new();
		for (shape, query_string) in sources {
			let result = engine.query_in_txn(&mut outer, &query_string, Params::None);
			if let Some(err) = result.error {
				return Err(err.into());
			}
			statements.extend(result.metrics.statements);
			let mut shape_columns: Vec<Columns> = Vec::new();
			for frame in result.frames {
				let columns = Columns::from(frame);
				let row_count = columns.row_count() as u64;
				total_rows = total_rows.saturating_add(row_count);
				if total_rows > max_rows {
					return Err(HydrateError::RowCapExceeded {
						cap: max_rows,
					});
				}
				shape_columns.push(columns);
			}
			source_frames.push((shape, shape_columns));
		}

		let now = DateTime::from_nanos(engine.clock().now_nanos());

		let flow_engine = self.state.flow_engine.write().unwrap();
		let flow_state = self.state.flow_states.remove(&flow_id).map(|(_, v)| v).unwrap_or_default();
		let primitive_query = self.state.multi.begin_query()?;
		let mut txn = FlowTransaction::ephemeral(
			version,
			primitive_query,
			self.state.catalog.clone(),
			flow_state,
			flow_engine.clock().clone(),
		);

		for (shape, shape_columns) in source_frames {
			for columns in shape_columns {
				for row_idx in 0..columns.row_count() {
					let row = columns.extract_row(row_idx);
					let diff = Diff::insert(row);
					let change = Change::from_shape(shape, version, vec![diff], now);
					flow_engine.process(&mut txn, change, flow_id)?;
				}
			}
		}

		txn.flush_operator_states()?;
		txn.merge_state();
		self.state.flow_states.insert(flow_id, txn.take_state());

		drop(flow_engine);

		self.state.delivery.commit_batch();

		drop(outer);

		let elapsed = hydrate_start.elapsed();
		let elapsed_nanos = elapsed.as_nanos() as i64;
		let total = ReifyDuration::from_nanoseconds(elapsed_nanos).unwrap_or_default();
		let fps: Vec<_> = statements.iter().map(|m| m.fingerprint).collect();
		let metrics = ExecutionMetrics {
			fingerprint: fingerprint_request(&fps),
			statements,
			total,
			compute: total,
		};

		let batches = self.state.store.drain(&sub_id, usize::MAX);
		Ok(HydrateOutcome {
			version,
			batches,
			metrics,
		})
	}
}

fn collect_source_descriptors(
	flow_engine: &Arc<RwLock<FlowEngine>>,
	flow_id: FlowId,
	catalog: &Catalog,
	outer: &mut QueryTransaction,
) -> StdResult<Vec<(ShapeId, String)>, HydrateError> {
	let fe = flow_engine.read().unwrap();
	let flow = fe.flows.get(&flow_id).cloned().ok_or(HydrateError::SubscriptionNotFound)?;
	drop(fe);

	let mut txn = Transaction::Query(outer);

	let mut out: Vec<(ShapeId, String)> = Vec::new();
	for node_id in flow.topological_order()? {
		let node = match flow.get_node(&node_id) {
			Some(n) => n,
			None => continue,
		};
		match &node.ty {
			FlowNodeType::SourceTable {
				table,
			} => {
				let t = catalog.get_table(&mut txn, *table)?;
				let ns = catalog.get_namespace(&mut txn, t.namespace)?;
				let mut q = format!("from {}::{}", ns.name(), t.name);
				append_pushdown(&mut q, walk_for_source_pushdown(&flow, &node_id));
				out.push((ShapeId::Table(*table), q));
			}
			FlowNodeType::SourceView {
				view,
			} => {
				let v = catalog.get_view(&mut txn, *view)?;
				let ns = catalog.get_namespace(&mut txn, v.namespace())?;
				let mut q = format!("from {}::{}", ns.name(), v.name());
				append_pushdown(&mut q, walk_for_source_pushdown(&flow, &node_id));
				out.push((ShapeId::View(*view), q));
			}
			FlowNodeType::SourceRingBuffer {
				ringbuffer,
			} => {
				let r = catalog.get_ringbuffer(&mut txn, *ringbuffer)?;
				let ns = catalog.get_namespace(&mut txn, r.namespace)?;
				let mut q = format!("from {}::{}", ns.name(), r.name);
				append_pushdown(&mut q, walk_for_source_pushdown(&flow, &node_id));
				out.push((ShapeId::RingBuffer(*ringbuffer), q));
			}
			_ => {
				if matches!(
					&node.ty,
					FlowNodeType::SourceInlineData { .. }
						| FlowNodeType::SourceFlow { .. } | FlowNodeType::SourceSeries { .. }
				) {
					return Err(HydrateError::UnsupportedSourceType);
				}
			}
		}
	}
	Ok(out)
}

struct SourcePushdown {
	parts: Vec<String>,
}

fn append_pushdown(q: &mut String, pd: SourcePushdown) {
	for part in pd.parts {
		q.push_str(" | ");
		q.push_str(&part);
	}
}

fn walk_for_source_pushdown(flow: &FlowDag, source_id: &FlowNodeId) -> SourcePushdown {
	let mut parts: Vec<String> = Vec::new();
	let mut current = *source_id;
	while let Some(node) = flow.get_node(&current) {
		if node.outputs.len() != 1 {
			break;
		}
		let next_id = node.outputs[0];
		let next = match flow.get_node(&next_id) {
			Some(n) => n,
			None => break,
		};
		match &next.ty {
			FlowNodeType::Filter {
				conditions,
			} => match render_filter_clause(conditions) {
				Some(clause) => parts.push(clause),
				None => {
					return SourcePushdown {
						parts: Vec::new(),
					};
				}
			},
			FlowNodeType::Take {
				limit,
			} => {
				parts.push(format!("take {}", limit));
			}
			_ => break,
		}
		current = next_id;
	}
	SourcePushdown {
		parts,
	}
}

fn render_filter_clause(conditions: &[Expression]) -> Option<String> {
	if conditions.is_empty() {
		return None;
	}
	let mut rendered: Vec<String> = Vec::with_capacity(conditions.len());
	for c in conditions {
		rendered.push(render_expr_rql(c)?);
	}
	Some(format!("filter {{ {} }}", rendered.join(" and ")))
}

fn render_expr_rql(expr: &Expression) -> Option<String> {
	match expr {
		Expression::Constant(c) => Some(render_constant_rql(c)),
		Expression::Column(ColumnExpression(col)) => Some(col.name.text().to_string()),
		Expression::Equal(e) => {
			Some(format!("({} == {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::NotEqual(e) => {
			Some(format!("({} != {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::GreaterThan(e) => {
			Some(format!("({} > {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::GreaterThanEqual(e) => {
			Some(format!("({} >= {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::LessThan(e) => {
			Some(format!("({} < {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::LessThanEqual(e) => {
			Some(format!("({} <= {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::And(e) => {
			Some(format!("({} and {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::Or(e) => Some(format!("({} or {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?)),
		_ => None,
	}
}

fn render_constant_rql(c: &ConstantExpression) -> String {
	match c {
		ConstantExpression::None {
			..
		} => "none".to_string(),
		ConstantExpression::Bool {
			fragment,
		} => fragment.text().to_string(),
		ConstantExpression::Number {
			fragment,
		} => fragment.text().to_string(),
		ConstantExpression::Text {
			fragment,
		} => format!("'{}'", fragment.text()),
		ConstantExpression::Temporal {
			fragment,
		} => fragment.text().to_string(),
	}
}

fn register_ephemeral_flow(
	engine: &mut FlowEngine,
	txn: &mut Transaction<'_>,
	flow: FlowDag,
	subscription_id: SubscriptionId,
	delivery: Arc<DeliveryBuffer>,
) -> Result<()> {
	for node_id in flow.topological_order()? {
		let node = flow.get_node(&node_id).unwrap();
		match &node.ty {
			FlowNodeType::SinkSubscription {
				..
			} => {
				let parent = engine
					.operators
					.get(&node.inputs[0])
					.expect("Parent operator not found")
					.clone();
				let op = EphemeralSinkSubscriptionOperator::new(
					parent,
					node_id,
					subscription_id,
					delivery.clone(),
				);
				engine.operators.insert(node_id, Arc::new(Operators::Custom(Box::new(op))));
			}
			_ => {
				engine.add(txn, &flow, node)?;
			}
		}
	}
	engine.analyzer.add(flow.clone());
	engine.flows.insert(flow.id, flow);
	Ok(())
}

pub struct SubscriptionSubsystem {
	consumer: PollConsumer<StandardEngine, SubscriptionCdcConsumer>,
	state: Arc<SubscriptionState>,
	running: bool,
}

impl SubscriptionSubsystem {
	pub fn new(
		engine: StandardEngine,
		cdc_store: CdcStore,
		store: Arc<SubscriptionStore>,
		runtime_context: RuntimeContext,
		custom_operators: Arc<HashMap<String, OperatorFactory>>,
		consumer_watermark: CdcConsumerWatermark,
	) -> Self {
		let catalog = engine.catalog();
		let executor = engine.executor();
		let event_bus = engine.event_bus().clone();
		let multi = engine.multi_owned();
		let actor_system = engine.actor_system();

		let flow_engine = Arc::new(RwLock::new(FlowEngine::new(
			catalog.clone(),
			executor,
			event_bus,
			runtime_context,
			custom_operators,
		)));

		let flow_states = Arc::new(DashMap::new());
		let hydration_versions = Arc::new(DashMap::new());
		let delivery = Arc::new(DeliveryBuffer::new(store.clone()));

		let state = Arc::new(SubscriptionState {
			store,
			flow_engine: flow_engine.clone(),
			flow_states: flow_states.clone(),
			hydration_versions: hydration_versions.clone(),
			subscription_flows: RwLock::new(HashMap::new()),
			delivery: delivery.clone(),
			multi: multi.clone(),
			catalog: catalog.clone(),
		});

		let cdc_consumer = SubscriptionCdcConsumer::new(
			flow_engine,
			multi,
			catalog,
			flow_states,
			hydration_versions,
			delivery,
		);

		let config = PollConsumerConfig::new(
			CdcConsumerId::new("__SUBSCRIPTION_CONSUMER"),
			"sub-subscription-poll",
			Duration::from_millis(10),
			None,
		)
		.with_consumer_watermark(consumer_watermark);

		let consumer = PollConsumer::new(config, engine, cdc_consumer, cdc_store, actor_system);

		Self {
			consumer,
			state,
			running: false,
		}
	}

	pub fn service_handle(&self) -> SubscriptionServiceRef {
		Arc::new(SubscriptionServiceImpl {
			state: self.state.clone(),
		})
	}

	pub fn store(&self) -> &Arc<SubscriptionStore> {
		&self.state.store
	}
}

impl Subsystem for SubscriptionSubsystem {
	fn name(&self) -> &'static str {
		"sub-subscription"
	}

	fn start(&mut self) -> Result<()> {
		if self.running {
			return Ok(());
		}
		self.consumer.start()?;
		self.running = true;
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running {
			return Ok(());
		}
		self.consumer.stop()?;
		self.running = false;
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running
	}

	fn health_status(&self) -> HealthStatus {
		if self.running {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

impl HasVersion for SubscriptionSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Ephemeral subscription subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

struct SubscriptionInspectorImpl {
	store: Arc<SubscriptionStore>,
}

impl SubscriptionInspector for SubscriptionInspectorImpl {
	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.store.active_subscriptions()
	}

	fn column_count(&self, id: &SubscriptionId) -> Option<usize> {
		self.store.column_names(id).map(|v| v.len())
	}

	fn inspect(&self, id: SubscriptionId) -> Option<Columns> {
		let batches = self.store.drain(&id, usize::MAX);
		if batches.is_empty() {
			let names = self.store.column_names(&id)?;
			let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
			return Some(Columns::from_rows(&name_refs, &[]));
		}
		if batches.len() == 1 {
			return Some(batches.into_iter().next().unwrap());
		}

		let first = &batches[0];
		let names: Vec<&str> = first.iter().map(|c| c.name().text()).collect();

		let mut all_rows = Vec::new();
		let mut all_row_numbers = Vec::new();

		for batch in &batches {
			for i in 0..batch.row_count() {
				all_rows.push(batch.get_row(i));
				if i < batch.row_numbers.len() {
					all_row_numbers.push(batch.row_numbers[i]);
				} else {
					all_row_numbers.push(RowNumber(0));
				}
			}
		}

		Some(Columns::from_rows(&names, &all_rows).with_row_numbers(all_row_numbers))
	}
}

pub struct SubscriptionSubsystemFactory;

impl SubsystemFactory for SubscriptionSubsystemFactory {
	fn provide_interceptors(&self, builder: InterceptorBuilder, _ioc: &IocContainer) -> InterceptorBuilder {
		builder
	}

	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let cdc_store = ioc.resolve::<CdcStore>()?;
		let runtime = ioc.resolve::<SharedRuntime>()?;

		let runtime_context = RuntimeContext::with_clock(runtime.clock().clone());
		let store = Arc::new(SubscriptionStore::new(1024));
		let custom_operators = Arc::new(HashMap::new());

		let consumer_watermark = CdcConsumerWatermark::new();
		ioc.register_service::<CdcConsumerWatermark>(consumer_watermark.clone());

		let subsystem = SubscriptionSubsystem::new(
			engine,
			cdc_store,
			store.clone(),
			runtime_context,
			custom_operators,
			consumer_watermark,
		);

		let service = subsystem.service_handle();
		ioc.register_service::<SubscriptionServiceRef>(service);
		ioc.register_service::<Arc<SubscriptionStore>>(store.clone());

		let inspector: SubscriptionInspectorRef = Arc::new(SubscriptionInspectorImpl {
			store,
		});
		ioc.register_service::<SubscriptionInspectorRef>(inspector);

		Ok(Box::new(subsystem))
	}
}

#[cfg(test)]
mod tests {
	use std::slice;

	use reifydb_rql::expression::parse_expression;

	use super::*;

	fn parse_one(rql: &str) -> Expression {
		parse_expression(rql).expect("parse").into_iter().next().expect("one expression")
	}

	#[test]
	fn render_filter_clause_emits_valid_rql_for_equality() {
		let expr = parse_one("kind == 'b'");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		assert_eq!(rendered, "filter { (kind == 'b') }");
	}

	#[test]
	fn render_filter_clause_emits_valid_rql_for_conjunction() {
		let expr = parse_one("kind == 'b' and value > 50");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		assert_eq!(rendered, "filter { ((kind == 'b') and (value > 50)) }");
	}

	#[test]
	fn render_filter_clause_joins_multiple_conditions_with_and() {
		let exprs = vec![parse_one("kind == 'b'"), parse_one("value > 50")];
		let rendered = render_filter_clause(&exprs).expect("renders");
		assert_eq!(rendered, "filter { (kind == 'b') and (value > 50) }");
	}

	#[test]
	fn render_filter_clause_renders_text_constant_with_single_quotes() {
		// Input uses double quotes; output must use RQL-parseable quotes (single).
		let expr = parse_one("base_mint == \"So11111111111111111111111111111111111111112\"");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		assert_eq!(rendered, "filter { (base_mint == 'So11111111111111111111111111111111111111112') }");
	}

	#[test]
	fn render_filter_clause_returns_none_for_unsupported_expression() {
		let expr = parse_one("upper(kind) == 'B'");
		assert!(render_filter_clause(slice::from_ref(&expr)).is_none());
	}

	#[test]
	fn render_filter_clause_returns_none_for_empty_conditions() {
		assert!(render_filter_clause(&[]).is_none());
	}

	#[test]
	fn render_constant_handles_each_constant_kind() {
		let bool_e = parse_one("true");
		let num_e = parse_one("42");
		let text_e = parse_one("'hello'");

		assert_eq!(render_expr_rql(&bool_e).unwrap(), "true");
		assert_eq!(render_expr_rql(&num_e).unwrap(), "42");
		assert_eq!(render_expr_rql(&text_e).unwrap(), "'hello'");
	}

	#[test]
	fn render_filter_clause_round_trips_through_rql_parser() {
		// The whole point of the renderer is that the result parses again as RQL.
		let expr = parse_one("base_mint == 'So11111111111111111111111111111111111111112'");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		// Strip the leading "filter { " and trailing " }" to get just the conditions.
		let inner = rendered.strip_prefix("filter { ").and_then(|s| s.strip_suffix(" }")).expect("structure");
		parse_expression(inner).expect("rendered RQL must reparse");
	}
}
