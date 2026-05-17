// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! `tracing_subscriber::Layer` that drives the profiler. On `on_new_span` for a tracked category it discovers the
//! ancestor `ScopeId` (from the task-local or the span ancestor chain), stashes a per-span extension carrying the
//! resolved scope and a category-specific field visitor, and on `on_close` builds a `MinimalSpanRecord` and pushes
//! it into the scope state. The sink is called per record (lock-free histogram observation) and again at scope
//! batch / close via the scope itself.

use std::{sync::Arc, time::Instant};

use tracing::{
	Metadata, Subscriber,
	span::{Attributes, Id, Record},
	subscriber::Interest,
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

use crate::{
	callsite,
	category::{CategorySet, ProfileCategory},
	intern::DimInterner,
	record::{DimIdx, MAX_EXTRAS, MinimalSpanRecord},
	scope::{ProfileScope, REGISTRY, ScopeState, active_scope},
	sink::ProfileSink,
	visit::FlowApplyFields,
};

pub struct ProfilerLayer {
	sink: Arc<dyn ProfileSink>,
	categories: CategorySet,
	interner: Arc<DimInterner>,
	ambient_scope: Arc<ScopeState>,
}

impl ProfilerLayer {
	pub fn new(sink: Arc<dyn ProfileSink>, categories: CategorySet, interner: Arc<DimInterner>) -> Self {
		let ambient_scope = ProfileScope::ambient("profile.global", Arc::clone(&sink));
		Self {
			sink,
			categories,
			interner,
			ambient_scope,
		}
	}
}

#[derive(Clone)]
struct SpanExt {
	category: ProfileCategory,
	scope: Arc<ScopeState>,
	callsite_id: u64,
	started_at: Instant,
	flow_fields: Option<FlowApplyFields>,
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
		let Some(c) = ProfileCategory::from_span_name(metadata.name()) else {
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
		let Some(c) = ProfileCategory::from_span_name(metadata.name()) else {
			return false;
		};
		self.categories.level_for(c).map(|max| max.admits(metadata.level())).unwrap_or(false)
	}

	fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
		let metadata = attrs.metadata();
		let Some(category) = ProfileCategory::from_span_name(metadata.name()) else {
			return;
		};
		let level_admitted =
			self.categories.level_for(category).map(|max| max.admits(metadata.level())).unwrap_or(false);
		if !level_admitted {
			return;
		}
		let scope = discover_scope(&ctx, id).unwrap_or_else(|| Arc::clone(&self.ambient_scope));
		let callsite_id = metadata_callsite_id(metadata);
		callsite::register(callsite_id, metadata.name());
		let mut flow_fields = None;
		if category == ProfileCategory::Flow && metadata.name() == "flow::engine::apply" {
			let mut v = FlowApplyFields::default();
			attrs.record(&mut v);
			flow_fields = Some(v);
		}
		let ext = SpanExt {
			category,
			scope,
			callsite_id,
			started_at: Instant::now(),
			flow_fields,
		};
		if let Some(span) = ctx.span(id) {
			span.extensions_mut().insert(ext);
		}
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
		}
	}

	fn on_close(&self, id: Id, ctx: Context<'_, S>) {
		let Some(span) = ctx.span(&id) else {
			return;
		};
		let entry = span.extensions_mut().remove::<SpanExt>();
		let Some(entry) = entry else {
			return;
		};
		let SpanExt {
			category,
			scope,
			callsite_id,
			started_at,
			flow_fields,
		} = entry;

		let mut record = MinimalSpanRecord::new(category, callsite_id, 0);
		match (category, &flow_fields) {
			(ProfileCategory::Flow, Some(f)) => {
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
				record.extras = extras;
			}
			_ => {
				let elapsed = started_at.elapsed().as_micros();
				record.duration_us = u32::try_from(elapsed).unwrap_or(u32::MAX);
			}
		}

		self.sink.on_span_record(&record);
		scope.attach_interner(Arc::clone(&self.interner));
		scope.push(record);
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex as StdMutex};

	use tracing::{debug_span, subscriber::with_default, trace_span};
	use tracing_subscriber::{Registry, layer::SubscriberExt};

	use super::*;
	use crate::{category::ProfileLevel, scope::ProfileScope, sink::ProfileSink, summary::ProfileSummary};

	#[derive(Default)]
	struct RecordingSink {
		records: StdMutex<Vec<MinimalSpanRecord>>,
		summaries: StdMutex<Vec<ProfileSummary>>,
	}

	impl ProfileSink for RecordingSink {
		fn on_span_record(&self, record: &MinimalSpanRecord) {
			self.records.lock().unwrap().push(*record);
		}
		fn on_scope_closed(&self, summary: &ProfileSummary) {
			self.summaries.lock().unwrap().push(summary.clone());
		}
		fn on_scope_batch(&self, summary: &ProfileSummary) {
			self.summaries.lock().unwrap().push(summary.clone());
		}
	}

	fn build_layer(sink: Arc<dyn ProfileSink>, categories: CategorySet) -> (ProfilerLayer, Arc<DimInterner>) {
		let interner = Arc::new(DimInterner::new());
		(ProfilerLayer::new(sink, categories, interner.clone()), interner)
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
		let recs = sink.records.lock().unwrap();
		assert_eq!(recs.len(), 1, "ambient scope must capture unscoped tracked spans for always-on profiling");
	}

	#[test]
	fn admits_at_or_below_category_level() {
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let categories = CategorySet::empty().with_level(ProfileCategory::Flow, ProfileLevel::Debug);
		let (layer, _interner) = build_layer(sink.clone(), categories);
		let subscriber = Registry::default().with(layer);

		with_default(subscriber, || {
			let handle = ProfileScope::start_with_sink("scope", sink.clone());
			handle.run_sync(|| {
				let trace_span =
					trace_span!("flow::engine::apply", node_id = "n", node_type = "trace_op");
				let _g1 = trace_span.enter();
				let debug_span = debug_span!("flow::engine::process_batch", batch_size = 1u64);
				let _g2 = debug_span.enter();
			});
			let _ = handle.finish();
		});

		let recs = sink.records.lock().unwrap();
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
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::empty().with(ProfileCategory::Query));
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfileScope::start_with_sink("scope", sink.clone());
			handle.run_sync(|| {
				let _ = trace_span!("flow::engine::apply", node_id = "n", node_type = "m");
			});
			let _ = handle.finish();
		});
		assert!(sink.records.lock().unwrap().is_empty());
	}

	#[test]
	fn flow_apply_captures_fields_and_interns_dims() {
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::all());
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfileScope::start_with_sink("scope", sink.clone());
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
		let recs = sink.records.lock().unwrap();
		assert_eq!(recs.len(), 1);
		let rec = recs[0];
		assert_eq!(rec.category(), ProfileCategory::Flow);
		assert_eq!(rec.duration_us, 200);
		assert_eq!(rec.extras[0], 10);
		assert_eq!(rec.extras[1], 5);
		assert_eq!(rec.extras[2], 3);
	}

	#[test]
	fn ancestor_walk_inherits_scope_across_spans() {
		let sink: Arc<RecordingSink> = Arc::new(RecordingSink::default());
		let (layer, _interner) = build_layer(sink.clone(), CategorySet::all());
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let handle = ProfileScope::start_with_sink("scope", sink.clone());
			handle.run_sync(|| {
				let outer = debug_span!("flow::engine::process_batch", batch_size = 3u64);
				let _g = outer.enter();
				let inner = trace_span!("flow::engine::apply", node_id = "n", node_type = "filter");
				let _g2 = inner.enter();
			});
			let _ = handle.finish();
		});
		let recs = sink.records.lock().unwrap();
		assert_eq!(recs.len(), 2);
	}
}
