// SPDX-License-Identifier: Apache-2.0

use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	wire::WireFormat as ClientWireFormat,
};
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_sub_core::wire_sink::{ChangeDelivery, SubscribedEntry, WireSink};
use reifydb_subscription::delivery::DeliveryResult;
use reifydb_value::value::frame::frame::Frame;
use tokio::sync::mpsc;
use tonic::Status;
use tracing::warn;

use crate::generated::{
	ChangeEntry, ChangeEvent, SubscribedEvent, SubscribedItem, SubscriptionClosedEvent, SubscriptionEvent,
	subscription_event,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WireFormat {
	#[default]
	Rbcf,
}

pub type SubscriptionRegistry = reifydb_sub_core::registry::SubscriptionRegistry<GrpcWireSink>;

#[derive(Clone)]
pub struct GrpcWireSink {
	tx: mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>,
}

impl GrpcWireSink {
	pub fn new(tx: mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>) -> Self {
		Self {
			tx,
		}
	}

	fn send(&self, event: subscription_event::Event) -> DeliveryResult {
		let event = SubscriptionEvent {
			event: Some(event),
		};
		if self.tx.send(Ok(event)).is_ok() {
			DeliveryResult::Delivered
		} else {
			DeliveryResult::Disconnected
		}
	}
}

impl WireSink for GrpcWireSink {
	type Format = WireFormat;

	fn client_wire_format(_format: Self::Format) -> ClientWireFormat {
		ClientWireFormat::Rbcf
	}

	fn send_subscribed(&self, subscriptions: &[SubscribedEntry]) -> DeliveryResult {
		self.send(subscription_event::Event::Subscribed(SubscribedEvent {
			subscriptions: subscriptions
				.iter()
				.map(|entry| SubscribedItem {
					index: entry.index as u32,
					subscription_id: entry.subscription_id.to_string(),
				})
				.collect(),
		}))
	}

	fn send_change(&self, format: Self::Format, entries: Vec<(SubscriptionId, Vec<Frame>)>) -> ChangeDelivery {
		let mut encoded = Vec::with_capacity(entries.len());
		let mut failed = Vec::new();
		for (sub_id, frames) in entries {
			match encode_change_payload(frames, format) {
				Ok(rbcf) => encoded.push(ChangeEntry {
					subscription_id: sub_id.to_string(),
					rbcf,
				}),
				Err(status) => {
					warn!(
						"Failed to encode change for {}, closing it: {}",
						sub_id,
						status.message()
					);
					failed.push(sub_id);
				}
			}
		}
		if !encoded.is_empty()
			&& matches!(
				self.send(subscription_event::Event::Change(ChangeEvent {
					entries: encoded,
				})),
				DeliveryResult::Disconnected
			) {
			return ChangeDelivery::Disconnected;
		}
		ChangeDelivery::Delivered {
			failed,
		}
	}

	fn send_subscription_closed(&self, subscription_id: SubscriptionId) -> DeliveryResult {
		self.send(subscription_event::Event::SubscriptionClosed(SubscriptionClosedEvent {
			subscription_id: subscription_id.to_string(),
		}))
	}
}

pub fn encode_change_payload(frames: Vec<Frame>, _format: WireFormat) -> Result<Vec<u8>, Status> {
	encode_frames(&frames, &EncodeOptions::fast())
		.map_err(|e| Status::internal(format!("failed to RBCF-encode change: {}", e)))
}

#[cfg(test)]
mod tests {
	use arrow_array::RecordBatch;
	use reifydb_core::value::batch::single_row;
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_sub_core::registry::PromoteResult;
	use reifydb_subscription::delivery::SubscriptionDelivery;
	use reifydb_value::value::{Value, diff_type::DiffType, duration::Duration, uuid::Uuid7};

	use super::*;

	type Events = mpsc::UnboundedReceiver<Result<SubscriptionEvent, Status>>;

	fn single_int_columns(name: &str, value: i64) -> RecordBatch {
		single_row([(name, Value::Int8(value))]).unwrap()
	}

	fn registry_with_sink() -> (SubscriptionRegistry, Uuid7, GrpcWireSink, Events) {
		let clock = Clock::Mock(MockClock::from_millis(1000));
		let rng = Rng::seeded(42);
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx, rx) = mpsc::unbounded_channel();
		(registry, connection_id, GrpcWireSink::new(tx), rx)
	}

	fn next_event(rx: &mut Events) -> subscription_event::Event {
		rx.try_recv().expect("an event").expect("ok").event.expect("a filled event")
	}

	#[tokio::test]
	async fn one_flush_coalesces_two_subscriptions_into_one_change_event() {
		let (registry, connection_id, sink, mut rx) = registry_with_sink();

		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);
		for sub in [sub_a, sub_b] {
			registry.subscribe(
				sub,
				connection_id,
				sink.clone(),
				WireFormat::Rbcf,
				None,
				Duration::zero(),
				Duration::zero(),
			);
		}

		assert!(matches!(
			registry.try_deliver(&sub_a, DiffType::Insert, single_int_columns("v", 1)),
			DeliveryResult::Delivered
		));
		assert!(matches!(
			registry.try_deliver(&sub_b, DiffType::Insert, single_int_columns("v", 2)),
			DeliveryResult::Delivered
		));

		assert!(rx.try_recv().is_err());

		registry.flush();

		let event = next_event(&mut rx);
		assert!(rx.try_recv().is_err());
		match event {
			subscription_event::Event::Change(c) => {
				let mut ids: Vec<String> =
					c.entries.iter().map(|e| e.subscription_id.clone()).collect();
				ids.sort();
				assert_eq!(ids, vec![sub_a.to_string(), sub_b.to_string()]);
			}
			other => panic!("expected Change, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_push_frames_for_remote_subscription() {
		let (registry, connection_id, sink, mut rx) = registry_with_sink();
		let sub_remote = SubscriptionId(42);
		registry.subscribe_remote(sub_remote, connection_id, sink, WireFormat::Rbcf);

		let frames = vec![Frame::from(single_int_columns("v", 99))];
		assert!(registry.push_frames(sub_remote, frames));

		registry.flush();
		match next_event(&mut rx) {
			subscription_event::Event::Change(c) => {
				assert_eq!(c.entries.len(), 1);
				assert_eq!(c.entries[0].subscription_id, sub_remote.to_string());
			}
			other => panic!("expected Change, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn closing_a_remote_subscription_sends_its_pending_change_then_the_close() {
		let (registry, connection_id, sink, mut rx) = registry_with_sink();
		let sub = SubscriptionId(123);
		registry.subscribe_remote(sub, connection_id, sink, WireFormat::Rbcf);
		assert!(registry.push_frames(sub, vec![Frame::from(single_int_columns("v", 1))]));

		assert!(registry.close(sub));

		match next_event(&mut rx) {
			subscription_event::Event::Change(c) => {
				assert_eq!(c.entries.len(), 1);
				assert_eq!(c.entries[0].subscription_id, sub.to_string());
			}
			other => panic!("expected the pending Change first, got {:?}", other),
		}
		match next_event(&mut rx) {
			subscription_event::Event::SubscriptionClosed(m) => {
				assert_eq!(m.subscription_id, sub.to_string())
			}
			other => panic!("expected SubscriptionClosed, got {:?}", other),
		}
		assert!(rx.try_recv().is_err(), "nothing may follow the close");
	}

	#[tokio::test]
	async fn test_warming_buffers_until_promote() {
		let (registry, connection_id, sink, mut rx) = registry_with_sink();

		let sub = SubscriptionId(7001);
		registry.subscribe(
			sub,
			connection_id,
			sink,
			WireFormat::Rbcf,
			Some(16),
			Duration::zero(),
			Duration::zero(),
		);

		assert!(matches!(
			registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 1)),
			DeliveryResult::Delivered
		));
		assert!(matches!(
			registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 2)),
			DeliveryResult::Delivered
		));
		registry.flush();
		assert!(rx.try_recv().is_err(), "no pushes while warming");

		match registry.promote_to_live(sub) {
			PromoteResult::Promoted(n) => assert_eq!(n, 2),
			other => panic!("unexpected promote result: {:?}", other),
		}
		assert!(rx.try_recv().is_err(), "promote queues the buffered changes for the next flush");

		registry.flush();
		match next_event(&mut rx) {
			subscription_event::Event::Change(c) => {
				assert_eq!(c.entries.len(), 1, "both buffered changes travel in the one entry");
				assert_eq!(c.entries[0].subscription_id, sub.to_string());
			}
			other => panic!("expected Change, got {:?}", other),
		}
		assert!(rx.try_recv().is_err());

		assert!(matches!(
			registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 3)),
			DeliveryResult::Delivered
		));
		registry.flush();
		assert!(matches!(next_event(&mut rx), subscription_event::Event::Change(_)));
	}

	#[tokio::test]
	async fn test_warming_overflow_marks_subscription() {
		let (registry, connection_id, sink, _rx) = registry_with_sink();

		let sub = SubscriptionId(7002);
		registry.subscribe(
			sub,
			connection_id,
			sink,
			WireFormat::Rbcf,
			Some(2),
			Duration::zero(),
			Duration::zero(),
		);

		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 1));
		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 2));
		registry.try_deliver(&sub, DiffType::Insert, single_int_columns("v", 3));

		match registry.promote_to_live(sub) {
			PromoteResult::Overflowed => {}
			other => panic!("expected Overflowed, got {:?}", other),
		}
	}
}
