// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::context::clock::{Clock, Instant};
use reifydb_value::reifydb_assertions;
use tracing::{
	Metadata, Subscriber,
	span::{Attributes, Id, Record},
	subscriber::Interest,
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use crate::{
	callsite,
	category::{CategorySet, ProfilerCategory},
	intern::DimInterner,
	record::{DimIdx, MAX_EXTRAS, MinimalSpanRecord},
	scope::{ProfilerScope, REGISTRY, ScopeState, active_scope},
	sink::ProfilerSink,
	visit::{FlowApplyFields, StateRangeFields},
};

pub struct ProfilerLayer {
	sink: Arc<dyn ProfilerSink>,
	categories: CategorySet,
	interner: Arc<DimInterner>,
	ambient_scope: Arc<ScopeState>,
	clock: Clock,
}

impl ProfilerLayer {
	pub fn new(
		sink: Arc<dyn ProfilerSink>,
		categories: CategorySet,
		interner: Arc<DimInterner>,
		clock: Clock,
	) -> Self {
		let ambient_scope = ProfilerScope::ambient("profiler.global", Arc::clone(&sink), &clock);
		Self {
			sink,
			categories,
			interner,
			ambient_scope,
			clock,
		}
	}
}

#[derive(Clone)]
struct SpanExt {
	category: ProfilerCategory,
	scope: Arc<ScopeState>,
	callsite_id: u64,
	started_at: Instant,
	child_us: u64,
	flow_fields: Option<FlowApplyFields>,
	state_fields: Option<StateRangeFields>,
}

fn metadata_callsite_id(metadata: &'static Metadata<'static>) -> u64 {
	let ptr: *const Metadata<'static> = metadata;
	ptr as usize as u64
}

fn discover_scope<S>(ctx: &Context<'_, S>, span_id: &Id) -> Option<Arc<ScopeState>>
where
	S: Subscriber + for<'a> LookupSpan<'a>,
{
	if let Some(span) = ctx.span(span_id) {
		for ancestor in span.scope() {
			let ext = ancestor.extensions();
			if let Some(ent) = ext.get::<SpanExt>() {
				return Some(Arc::clone(&ent.scope));
			}
		}
	}
	let id = active_scope()?;
	REGISTRY.get(id)
}

impl<S> Layer<S> for ProfilerLayer
where
	S: Subscriber + for<'a> LookupSpan<'a>,
{
	fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
		if !metadata.is_span() {
			return Interest::never();
		}
		let Some(c) = ProfilerCategory::from_span_name(metadata.name()) else {
			return Interest::never();
		};
		let Some(max) = self.categories.level_for(c) else {
			return Interest::never();
		};
		if max.admits(metadata.level()) {
			Interest::always()
		} else {
			Interest::never()
		}
	}

	fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
		if !metadata.is_span() {
			return false;
		}
		let Some(c) = ProfilerCategory::from_span_name(metadata.name()) else {
			return false;
		};
		self.categories.level_for(c).map(|max| max.admits(metadata.level())).unwrap_or(false)
	}

	fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
		let metadata = attrs.metadata();
		let Some(category) = self.resolve_admitted_category(metadata) else {
			return;
		};
		let scope = self.discover_span_scope(&ctx, id);
		let callsite_id = self.register_span_callsite(metadata);
		let flow_fields = self.extract_flow_fields(category, attrs, metadata);
		let state_fields = self.extract_state_fields(category, attrs, metadata);
		self.insert_span_ext(&ctx, id, category, scope, callsite_id, flow_fields, state_fields);
	}

	fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
		let Some(span) = ctx.span(id) else {
			return;
		};
		let mut ext = span.extensions_mut();
		if let Some(entry) = ext.get_mut::<SpanExt>() {
			if let Some(fields) = entry.flow_fields.as_mut() {
				values.record(fields);
			}
			if let Some(fields) = entry.state_fields.as_mut() {
				values.record(fields);
			}
		}
	}

	fn on_close(&self, id: Id, ctx: Context<'_, S>) {
		let Some(entry) = self.take_span_ext(&ctx, &id) else {
			return;
		};
		let record = self.build_record(&entry);
		self.charge_parent(&ctx, &id, record.duration_us as u64);
		self.emit_record(&entry.scope, record);
	}
}

impl ProfilerLayer {
	#[inline]
	fn resolve_admitted_category(&self, metadata: &'static Metadata<'static>) -> Option<ProfilerCategory> {
		let category = ProfilerCategory::from_span_name(metadata.name())?;
		let max = self.categories.level_for(category)?;
		if max.admits(metadata.level()) {
			Some(category)
		} else {
			None
		}
	}

	#[inline]
	fn register_span_callsite(&self, metadata: &'static Metadata<'static>) -> u64 {
		let callsite_id = metadata_callsite_id(metadata);
		callsite::register(callsite_id, metadata.name());
		callsite_id
	}

	#[inline]
	fn extract_flow_fields(
		&self,
		category: ProfilerCategory,
		attrs: &Attributes<'_>,
		metadata: &'static Metadata<'static>,
	) -> Option<FlowApplyFields> {
		if category == ProfilerCategory::Flow && metadata.name() == "flow::engine::apply" {
			let mut fields = FlowApplyFields::default();
			attrs.record(&mut fields);
			Some(fields)
		} else {
			None
		}
	}

	#[inline]
	fn extract_state_fields(
		&self,
		category: ProfilerCategory,
		attrs: &Attributes<'_>,
		metadata: &'static Metadata<'static>,
	) -> Option<StateRangeFields> {
		if category == ProfilerCategory::Flow && metadata.name() == "flow::state::range_limited" {
			let mut fields = StateRangeFields::default();
			attrs.record(&mut fields);
			Some(fields)
		} else {
			None
		}
	}

	#[inline]
	fn build_record(&self, entry: &SpanExt) -> MinimalSpanRecord {
		let mut record = MinimalSpanRecord::new(entry.category, entry.callsite_id, 0);
		match (entry.category, &entry.flow_fields) {
			(ProfilerCategory::Flow, Some(f)) => {
				record.duration_us = u32::try_from(f.apply_time_us).unwrap_or(u32::MAX);
				let mut dims: [DimIdx; 2] = [0, 0];
				if !f.node_type.is_empty() {
					dims[0] = self.interner.intern(&f.node_type);
				}
				if !f.node_id.is_empty() {
					dims[1] = self.interner.intern(&f.node_id);
				}
				record.dim_indices = dims;
				let mut extras = [0u64; MAX_EXTRAS];
				extras[0] = f.input_rows;
				extras[1] = f.output_rows;
				extras[2] = f.lock_wait_us;
				extras[3] = f.store_reads;
				record.extras = extras;
			}
			_ => {
				reifydb_assertions! {
					let now = self.clock.instant();
					assert!(
						now >= entry.started_at,
						"span close observed a clock instant before its start, so elapsed() would saturate \
						 to zero and silently report a missing duration for an untracked-flow span; the \
						 profiler relies on a monotonic clock for span timing (now_us={}, started_us={})",
						now.elapsed().as_micros(),
						entry.started_at.elapsed().as_micros()
					);
				}
				let elapsed = entry.started_at.elapsed().as_micros();
				record.duration_us = u32::try_from(elapsed).unwrap_or(u32::MAX);
			}
		}
		if let Some(s) = &entry.state_fields {
			record.extras[0] = s.rows_fetched;
			record.extras[1] = s.rows_tombstoned;
			if !s.site.is_empty() {
				record.dim_indices[0] = self.interner.intern(&s.site);
			}
			if let Some(operator) = s.operator_id {
				record.dim_indices[1] = self.interner.intern(&format!("op{operator}"));
			}
		}
		record.self_us =
			u32::try_from((record.duration_us as u64).saturating_sub(entry.child_us)).unwrap_or(u32::MAX);
		record
	}

	#[inline]
	fn charge_parent<S>(&self, ctx: &Context<'_, S>, id: &Id, elapsed_us: u64)
	where
		S: Subscriber + for<'a> LookupSpan<'a>,
	{
		let Some(span) = ctx.span(id) else {
			return;
		};
		let Some(parent) = span.parent() else {
			return;
		};
		for ancestor in parent.scope() {
			let mut ext = ancestor.extensions_mut();
			if let Some(entry) = ext.get_mut::<SpanExt>() {
				entry.child_us = entry.child_us.saturating_add(elapsed_us);
				return;
			}
		}
	}

	#[inline]
	fn emit_record(&self, scope: &ScopeState, record: MinimalSpanRecord) {
		self.sink.on_span_record(&record);
		scope.attach_interner(Arc::clone(&self.interner));
		scope.push(record);
	}
}

impl ProfilerLayer {
	#[inline]
	fn discover_span_scope<S>(&self, ctx: &Context<'_, S>, id: &Id) -> Arc<ScopeState>
	where
		S: Subscriber + for<'a> LookupSpan<'a>,
	{
		discover_scope(ctx, id).unwrap_or_else(|| Arc::clone(&self.ambient_scope))
	}

	#[inline]
	fn insert_span_ext<S>(
		&self,
		ctx: &Context<'_, S>,
		id: &Id,
		category: ProfilerCategory,
		scope: Arc<ScopeState>,
		callsite_id: u64,
		flow_fields: Option<FlowApplyFields>,
		state_fields: Option<StateRangeFields>,
	) where
		S: Subscriber + for<'a> LookupSpan<'a>,
	{
		let ext = SpanExt {
			category,
			scope,
			callsite_id,
			started_at: self.clock.instant(),
			child_us: 0,
			flow_fields,
			state_fields,
		};
		if let Some(span) = ctx.span(id) {
			span.extensions_mut().insert(ext);
		}
	}

	#[inline]
	fn take_span_ext<S>(&self, ctx: &Context<'_, S>, id: &Id) -> Option<SpanExt>
	where
		S: Subscriber + for<'a> LookupSpan<'a>,
	{
		let span = ctx.span(id)?;
		span.extensions_mut().remove::<SpanExt>()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_runtime::{context::clock::Clock, sync::mutex::Mutex as StdMutex};
	use tracing::{debug_span, field::Empty, subscriber::with_default, trace_span};
	use tracing_subscriber::{Registry, layer::SubscriberExt};

	use super::*;
	use crate::{
		category::ProfilerLevel, record::DIM_UNSET, scope::ProfilerScope, sink::ProfilerSink,
		summary::ProfilerSummary,
	};

	#[derive(Default)]
	struct RecordingSink {
		records: StdMutex<Vec<MinimalSpanRecord>>,
		summaries: StdMutex<Vec<ProfilerSummary>>,
	}

	impl ProfilerSink for RecordingSink {
		fn on_span_record(&self, record: &MinimalSpanRecord) {
			self.records.lock().push(*record);
		}
		fn on_scope_closed(&self, summary: &ProfilerSummary) {
			self.summaries.lock().push(summary.clone());
		}
		fn on_scope_batch(&self, summary: &ProfilerSummary) {
			self.summaries.lock().push(summary.clone());
		}
	}

	fn build_layer(sink: Arc<dyn ProfilerSink>, categories: CategorySet) -> (ProfilerLayer, Arc<DimInterner>) {
		let interner = Arc::new(DimInterner::new());
		(ProfilerLayer::new(sink, categories, interner.clone(), Clock::Real), interner)
	}

	#[test]
	fn out_of_scope_span_captured_via_ambient_scope() {
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::all());
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let span = debug_span!("flow::engine::apply", node_id = "n1", node_type = "map");
			let _g = span.enter();
		});
		let recs = sink.records.lock();
		assert_eq!(recs.len(), 1, "ambient scope must capture unscoped tracked spans for always-on profiling");
	}

	#[test]
	fn admits_at_or_below_category_level() {
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let categories = CategorySet::empty().with_level(ProfilerCategory::Flow, ProfilerLevel::Debug);
		let (layer, _interner) = build_layer(sink.clone(), categories);
		let subscriber = Registry::default().with(layer);

		with_default(subscriber, || {
			let handle = ProfilerScope::start_with_sink("scope", sink.clone(), Clock::Real);
			handle.run_sync(|| {
				let trace_span =
					trace_span!("flow::engine::apply", node_id = "n", node_type = "trace_op");
				let _g1 = trace_span.enter();
				let debug_span = debug_span!("flow::engine::process_batch", batch_size = 1u64);
				let _g2 = debug_span.enter();
			});
			let _ = handle.finish();
		});

		let recs = sink.records.lock();
		assert_eq!(
			recs.len(),
			1,
			"DEBUG-level filter must admit process_batch (debug) but reject apply (trace), got: {:?}",
			recs
		);
	}

	#[test]
	fn disabled_category_short_circuits() {
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::empty().with(ProfilerCategory::Query));
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfilerScope::start_with_sink("scope", sink.clone(), Clock::Real);
			handle.run_sync(|| {
				let _ = trace_span!("flow::engine::apply", node_id = "n", node_type = "m");
			});
			let _ = handle.finish();
		});
		assert!(sink.records.lock().is_empty());
	}

	#[test]
	fn flow_apply_captures_fields_and_interns_dims() {
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::all());
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfilerScope::start_with_sink("scope", sink.clone(), Clock::Real);
			handle.run_sync(|| {
				let span = trace_span!(
					"flow::engine::apply",
					node_id = "n1",
					node_type = "map",
					input_rows = 10u64,
					output_rows = 5u64,
					apply_time_us = 200u64,
					lock_wait_us = 3u64,
				);
				let _g = span.enter();
			});
			let _summary = handle.finish();
		});
		let recs = sink.records.lock();
		assert_eq!(recs.len(), 1);
		let rec = recs[0];
		assert_eq!(rec.category(), ProfilerCategory::Flow);
		assert_eq!(rec.duration_us, 200);
		assert_eq!(rec.extras[0], 10);
		assert_eq!(rec.extras[1], 5);
		assert_eq!(rec.extras[2], 3);
	}

	#[test]
	fn state_range_interns_its_site_without_borrowing_the_apply_duration_override() {
		// state_range shares the Flow category with flow::engine::apply, but it has no
		// apply_time_us to override its duration with. If it took the apply branch the span
		// would report 0us and every call site would look free; if it skipped interning the
		// site the 11 call sites would collapse into one undifferentiated row, which is the
		// whole reason the dimension exists. The site must land in dim 0 and the duration must
		// come from the wall clock.
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, interner) = build_layer(sink.clone(), CategorySet::all());
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfilerScope::start_with_sink("scope", sink.clone(), Clock::Real);
			handle.run_sync(|| {
				let span = trace_span!(
					"flow::state::range_limited",
					operator_id = 7u64,
					site = "timer::hydrate_probe"
				);
				let _g = span.enter();
			});
			let _summary = handle.finish();
		});
		let recs = sink.records.lock();
		assert_eq!(recs.len(), 1);
		let rec = recs[0];
		assert_eq!(rec.category(), ProfilerCategory::Flow);
		assert_eq!(
			interner.resolve(rec.dim_indices[0]).as_deref(),
			Some("timer::hydrate_probe"),
			"the call site must be interned into dim 0 so the profile table can split by it"
		);
		assert_ne!(
			rec.dim_indices[0], DIM_UNSET,
			"an unset dim would render as <no-dims> and merge every call site into one row"
		);
		assert_eq!(
			interner.resolve(rec.dim_indices[1]).as_deref(),
			Some("op7"),
			"the operator must occupy dim 1 so one hot wheel cannot hide inside a site-wide average"
		);
	}

	#[test]
	fn state_range_carries_its_physical_row_counts_into_extras() {
		// A scan's cost tracks rows the storage fetched, not rows it returned: the timer probe
		// asks for one row and sqlite hands back every tombstone in the prefix because LIMIT
		// applies after the WHERE. Without these two counters the profile shows a slow probe and
		// no way to tell an expensive scan from an expensive tombstone tax.
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::all());
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfilerScope::start_with_sink("scope", sink.clone(), Clock::Real);
			handle.run_sync(|| {
				let span = trace_span!(
					"flow::state::range_limited",
					operator_id = 7u64,
					site = "timer::hydrate_probe",
					rows_fetched = 431u64,
					rows_tombstoned = 430u64
				);
				let _g = span.enter();
			});
			let _summary = handle.finish();
		});
		let recs = sink.records.lock();
		assert_eq!(recs.len(), 1);
		let rec = recs[0];
		assert_eq!(rec.extras[0], 431, "rows fetched from storage must reach extras[0]");
		assert_eq!(rec.extras[1], 430, "of which tombstones must reach extras[1]");
	}

	#[test]
	fn flow_apply_store_reads_recorded_late_lands_in_extras() {
		// store_reads is a delta around apply, so it is recorded after the operator ran and
		// arrives through on_record. Capturing only creation-time attributes would report
		// gets=0 for every operator and lose per-node read attribution.
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::all());
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfilerScope::start_with_sink("scope", sink.clone(), Clock::Real);
			handle.run_sync(|| {
				let span = trace_span!(
					"flow::engine::apply",
					node_id = "n1",
					node_type = "window",
					input_rows = 4u64,
					output_rows = 2u64,
					apply_time_us = 100u64,
					lock_wait_us = 1u64,
					store_reads = Empty,
				);
				let _g = span.enter();
				span.record("store_reads", 37u64);
			});
			let _ = handle.finish();
		});
		let recs = sink.records.lock();
		assert_eq!(recs.len(), 1);
		let rec = recs[0];
		assert_eq!(rec.category(), ProfilerCategory::Flow);
		assert_eq!(rec.extras[0], 4);
		assert_eq!(rec.extras[1], 2);
		assert_eq!(rec.extras[3], 37, "a late-recorded store_reads must land in extras[3]");
	}

	#[test]
	fn ancestor_walk_inherits_scope_across_spans() {
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::all());
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfilerScope::start_with_sink("scope", sink.clone(), Clock::Real);
			handle.run_sync(|| {
				let outer = debug_span!("flow::engine::process_batch", batch_size = 3u64);
				let _g = outer.enter();
				let inner = trace_span!("flow::engine::apply", node_id = "n", node_type = "filter");
				let _g2 = inner.enter();
			});
			let _ = handle.finish();
		});
		let recs = sink.records.lock();
		assert_eq!(recs.len(), 2);
	}
}
