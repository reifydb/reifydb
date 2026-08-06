// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt;

use tracing::field::{Field, Visit};

use crate::{
	record::{MAX_DIMENSIONS, MAX_EXTRAS},
	spec::{DimSource, SpanSpec},
};

#[derive(Clone, Debug)]
pub struct SpecFields {
	spec: &'static SpanSpec,
	dims: [String; MAX_DIMENSIONS],
	extras: [u64; MAX_EXTRAS],
	duration_override: Option<u64>,
}

impl SpecFields {
	pub fn new(spec: &'static SpanSpec) -> Self {
		Self {
			spec,
			dims: Default::default(),
			extras: [0; MAX_EXTRAS],
			duration_override: None,
		}
	}

	pub fn dims(&self) -> &[String; MAX_DIMENSIONS] {
		&self.dims
	}

	pub fn extras(&self) -> &[u64; MAX_EXTRAS] {
		&self.extras
	}

	pub fn duration_override(&self) -> Option<u64> {
		self.duration_override
	}

	fn set_text_dim(&mut self, name: &str, value: &str) {
		for (slot, source) in self.spec.dims.iter().enumerate() {
			if matches!(source, DimSource::Text(field) if *field == name) {
				self.dims[slot].replace_range(.., value);
			}
		}
	}
}

impl Visit for SpecFields {
	fn record_u64(&mut self, field: &Field, value: u64) {
		let name = field.name();
		if self.spec.duration_override == Some(name) {
			self.duration_override = Some(value);
		}
		for (slot, source) in self.spec.dims.iter().enumerate() {
			if let DimSource::Number {
				field: dim_field,
				prefix,
			} = source && *dim_field == name
			{
				self.dims[slot].clear();
				self.dims[slot].push_str(prefix);
				self.dims[slot].push_str(&value.to_string());
			}
		}
		for (slot, extra) in self.spec.extras.iter().enumerate() {
			if *extra == name {
				self.extras[slot] = value;
			}
		}
	}

	fn record_i64(&mut self, field: &Field, value: i64) {
		if value >= 0 {
			self.record_u64(field, value as u64);
		}
	}

	fn record_str(&mut self, field: &Field, value: &str) {
		self.set_text_dim(field.name(), value);
	}

	fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
		let rendered = format!("{:?}", value);
		self.set_text_dim(field.name(), rendered.trim_matches('"'));
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_runtime::sync::mutex::Mutex;
	use tracing::{
		Subscriber, debug_span,
		span::{Attributes, Id},
		subscriber::with_default,
	};
	use tracing_subscriber::{
		Layer, Registry,
		layer::{Context, SubscriberExt},
		registry::LookupSpan,
	};

	use super::*;
	use crate::spec::spec_for;

	struct CaptureLayer {
		captured: Arc<Mutex<Option<SpecFields>>>,
	}

	impl<S> Layer<S> for CaptureLayer
	where
		S: Subscriber + for<'a> LookupSpan<'a>,
	{
		fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
			let Some(name) = ctx.span(id).map(|s| s.name()) else {
				return;
			};
			let Some(spec) = spec_for(name) else {
				return;
			};
			let mut v = SpecFields::new(spec);
			attrs.record(&mut v);
			*self.captured.lock() = Some(v);
		}
	}

	fn capture(build: impl FnOnce()) -> SpecFields {
		let captured = Arc::new(Mutex::new(None));
		let layer = CaptureLayer {
			captured: captured.clone(),
		};
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, build);
		let taken = captured.lock().clone();
		taken.expect("a span matching a spec must be captured")
	}

	#[test]
	fn apply_fields_land_in_the_slots_its_spec_declares() {
		// Slot order is what the formatter prints as lock=/io=/gets=, so a field landing in the
		// wrong slot silently relabels the counters rather than failing.
		let captured = capture(|| {
			let _span = debug_span!(
				"flow::engine::apply",
				node_type = "map",
				operator_id = 79u64,
				input_rows = 10u64,
				output_rows = 7u64,
				apply_time_us = 250u64,
				lock_wait_us = 5u64,
				store_reads = 3u64,
			);
		});
		assert_eq!(captured.dims()[0], "map");
		assert_eq!(captured.dims()[1], "op79", "operator_id must label the second dimension");
		assert_eq!(captured.extras(), &[10, 7, 5, 3]);
		assert_eq!(captured.duration_override(), Some(250));
	}

	#[test]
	fn a_span_without_a_duration_override_field_reports_none() {
		// build_record falls back to the wall clock only when this is None. If an absent
		// apply_time_us defaulted to Some(0), every span without one would report as free.
		let captured = capture(|| {
			let _span = debug_span!(
				"flow::state::range_limited",
				site = "timer::hydrate_probe",
				operator_id = 7u64,
				rows_fetched = 12u64,
				rows_tombstoned = 4u64,
			);
		});
		assert_eq!(captured.duration_override(), None);
		assert_eq!(captured.dims()[0], "timer::hydrate_probe");
		assert_eq!(captured.dims()[1], "op7");
		assert_eq!(captured.extras()[0], 12);
		assert_eq!(captured.extras()[1], 4);
	}

	#[test]
	fn a_debug_formatted_dimension_loses_its_quotes() {
		// Fields recorded with ?value arrive through record_debug wrapped in quotes. Leaving them
		// would render the row as site@"reclaim::range" and split one logical dimension into two
		// labels depending on how the call site happened to record it.
		let captured = capture(|| {
			let _span =
				debug_span!("flow::state::range_limited", site = ?"reclaim::range", operator_id = 1u64);
		});
		assert_eq!(captured.dims()[0], "reclaim::range");
	}

	#[test]
	fn a_field_the_spec_does_not_declare_is_ignored() {
		// Spans carry fields for logging that are neither dimensions nor counters. One of them
		// landing in a slot would corrupt an unrelated column.
		let captured = capture(|| {
			let _span = debug_span!(
				"flow::state::range_limited",
				site = "reclaim::range",
				operator_id = 1u64,
				rows_fetched = 5u64,
				rows_tombstoned = 0u64,
				num_parents = 9u64,
			);
		});
		assert_eq!(captured.extras()[2], 0, "an undeclared field must not reach a slot");
		assert_eq!(captured.extras()[3], 0);
	}
}
