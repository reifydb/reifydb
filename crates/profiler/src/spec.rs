// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Write;

use crate::{format::fmt_us, record::AggregateRecord};

#[derive(Debug)]
pub enum DimSource {
	Text(&'static str),

	Number {
		field: &'static str,
		prefix: &'static str,
	},
}

impl DimSource {
	pub fn field(&self) -> &'static str {
		match self {
			DimSource::Text(field) => field,
			DimSource::Number {
				field,
				..
			} => field,
		}
	}
}

#[derive(Debug)]
pub struct SpanSpec {
	pub name: &'static str,

	pub duration_override: Option<&'static str>,
	pub dims: &'static [DimSource],
	pub extras: &'static [&'static str],
	pub render: Option<fn(&AggregateRecord, &mut String)>,
}

static SPECS: &[SpanSpec] = &[
	SpanSpec {
		name: "flow::engine::apply",
		duration_override: Some("apply_time_us"),
		dims: &[
			DimSource::Text("node_type"),
			DimSource::Number {
				field: "operator_id",
				prefix: "op",
			},
		],
		extras: &["input_rows", "output_rows", "lock_wait_us"],
		render: Some(render_apply),
	},
	SpanSpec {
		name: "flow::state::range_limited",
		duration_override: None,
		dims: &[
			DimSource::Text("site"),
			DimSource::Number {
				field: "operator_id",
				prefix: "op",
			},
		],
		extras: &["rows_fetched", "rows_tombstoned"],
		render: Some(render_state_range),
	},
	SpanSpec {
		name: "flow::state::range",
		duration_override: None,
		dims: &[
			DimSource::Text("site"),
			DimSource::Number {
				field: "operator_id",
				prefix: "op",
			},
		],
		extras: &["rows_fetched", "rows_tombstoned"],
		render: Some(render_state_range),
	},
];

pub fn spec_for(name: &str) -> Option<&'static SpanSpec> {
	SPECS.iter().find(|spec| spec.name == name)
}

fn render_apply(record: &AggregateRecord, out: &mut String) {
	let e = record.extras();
	let _ = write!(out, " lock={} io={}->{}", fmt_us(e[2]), e[0], e[1]);
}

fn render_state_range(record: &AggregateRecord, out: &mut String) {
	let e = record.extras();
	let dead = match e[0] {
		0 => 0.0,
		fetched => e[1] as f64 * 100.0 / fetched as f64,
	};
	let per_call = match record.calls {
		0 => 0,
		calls => e[0] / calls,
	};
	let _ = write!(out, " fetched={} tomb={} dead={:.0}% rows/call={}", e[0], e[1], dead, per_call);
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::record::{MAX_DIMENSIONS, MAX_EXTRAS};

	#[test]
	fn every_spec_fits_the_record_it_fills() {
		// dims and extras are copied into fixed-size arrays on MinimalSpanRecord. A spec that
		// declared more than the record holds would silently drop the overflow at the tail, so
		// the widest column of a hot span would go missing rather than fail loudly.
		for spec in SPECS {
			assert!(
				spec.dims.len() <= MAX_DIMENSIONS,
				"{} declares {} dimensions but a record holds {MAX_DIMENSIONS}",
				spec.name,
				spec.dims.len()
			);
			assert!(
				spec.extras.len() <= MAX_EXTRAS,
				"{} declares {} extras but a record holds {MAX_EXTRAS}",
				spec.name,
				spec.extras.len()
			);
		}
	}

	#[test]
	fn a_spec_name_resolves_only_itself() {
		// spec_for keys off the span name, so a duplicated or prefix-colliding entry would hand
		// the layer the wrong field wiring and mislabel every row of that span.
		for spec in SPECS {
			let found = spec_for(spec.name).expect("declared spec must resolve");
			assert_eq!(found.name, spec.name);
		}
		assert!(spec_for("flow::engine::process_batch").is_none(), "an unlisted span must opt out");
	}

	#[test]
	fn only_a_deliberate_span_overrides_its_own_duration() {
		// A duration override makes the span report something narrower than its elapsed time,
		// which hides every cost between the span boundary and the overridden measurement. It is
		// load-bearing for flow::engine::apply and must not spread by copy-paste.
		let overriding: Vec<&str> =
			SPECS.iter().filter(|s| s.duration_override.is_some()).map(|s| s.name).collect();
		assert_eq!(
			overriding,
			vec!["flow::engine::apply"],
			"a new duration override needs an explicit decision, not a default"
		);
	}
}
