// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

use tracing::field::{Field, Visit};

#[derive(Default, Clone, Debug)]
pub struct FlowApplyFields {
	pub node_id: String,
	pub node_type: String,
	pub input_rows: u64,
	pub output_rows: u64,
	pub apply_time_us: u64,
	pub lock_wait_us: u64,
}

impl Visit for FlowApplyFields {
	fn record_u64(&mut self, field: &Field, value: u64) {
		match field.name() {
			"input_rows" => self.input_rows = value,
			"output_rows" => self.output_rows = value,
			"apply_time_us" => self.apply_time_us = value,
			"lock_wait_us" => self.lock_wait_us = value,
			_ => {}
		}
	}

	fn record_i64(&mut self, field: &Field, value: i64) {
		if value >= 0 {
			self.record_u64(field, value as u64);
		}
	}

	fn record_str(&mut self, field: &Field, value: &str) {
		match field.name() {
			"node_id" => self.node_id.replace_range(.., value),
			"node_type" => self.node_type.replace_range(.., value),
			_ => {}
		}
	}

	fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
		match field.name() {
			"node_id" => {
				self.node_id.clear();
				self.node_id.push_str(&format!("{:?}", value));
			}
			"node_type" => {
				self.node_type.clear();
				self.node_type.push_str(format!("{:?}", value).trim_matches('"'));
			}
			_ => {}
		}
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

	struct CaptureLayer {
		captured: Arc<Mutex<Option<FlowApplyFields>>>,
	}

	impl<S> Layer<S> for CaptureLayer
	where
		S: Subscriber + for<'a> LookupSpan<'a>,
	{
		fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
			let mut v = FlowApplyFields::default();
			attrs.record(&mut v);
			*self.captured.lock() = Some(v);
		}
	}

	#[test]
	fn extracts_flow_apply_fields() {
		let captured = Arc::new(Mutex::new(None));
		let layer = CaptureLayer {
			captured: captured.clone(),
		};
		let subscriber = Registry::default().with(layer);
		with_default(subscriber, || {
			let _span = debug_span!(
				"flow::engine::apply",
				node_id = "n1",
				node_type = "map",
				input_rows = 10u64,
				output_rows = 7u64,
				apply_time_us = 250u64,
				lock_wait_us = 5u64,
			);
		});
		let captured = captured.lock().clone().unwrap();
		assert_eq!(captured.node_id, "n1");
		assert_eq!(captured.node_type, "map");
		assert_eq!(captured.input_rows, 10);
		assert_eq!(captured.output_rows, 7);
		assert_eq!(captured.apply_time_us, 250);
		assert_eq!(captured.lock_wait_us, 5);
	}
}
