// SPDX-License-Identifier: AGPL-3.0-or-later

use reifydb_client::{RawChangePayload, WireFormat as ClientWireFormat};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_sub_server::subscription::wire_sink::{BatchSubscribedMember, WireSink};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_value::value::frame::frame::Frame;
use reifydb_wire_format::{encode::encode_frames, options::EncodeOptions};
use tokio::sync::mpsc;
use tonic::Status;

use crate::{
	convert::frames_to_proto,
	generated::{
		BatchChangeEntry, BatchChangeEvent, BatchMember, BatchMemberClosedEvent, BatchSubscribedEvent,
		BatchSubscriptionEvent, ChangeEvent, Format, FramesPayload, SubscribedEvent, SubscriptionEvent,
		batch_subscription_event, change_event, subscription_event,
	},
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WireFormat {
	#[default]
	Proto,
	Rbcf,
}

impl WireFormat {
	pub fn from_proto_i32(format: i32) -> Self {
		match Format::try_from(format).unwrap_or(Format::Unspecified) {
			Format::Rbcf => WireFormat::Rbcf,
			Format::Proto | Format::Unspecified => WireFormat::Proto,
		}
	}
}

pub type SubscriptionRegistry = reifydb_sub_server::subscription::registry::SubscriptionRegistry<GrpcWireSink>;

#[derive(Clone)]
pub enum GrpcWireSink {
	Single(mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>),
	Batch(mpsc::UnboundedSender<Result<BatchSubscriptionEvent, Status>>),
}

impl WireSink for GrpcWireSink {
	type Format = WireFormat;

	fn client_wire_format(format: Self::Format) -> ClientWireFormat {
		match format {
			WireFormat::Rbcf => ClientWireFormat::Rbcf,
			WireFormat::Proto => ClientWireFormat::Proto,
		}
	}

	fn send_subscribed(&self, sub_id: SubscriptionId) -> DeliveryResult {
		match self {
			Self::Single(tx) => {
				let event = SubscriptionEvent {
					event: Some(subscription_event::Event::Subscribed(SubscribedEvent {
						subscription_id: sub_id.0.to_string(),
					})),
				};
				if tx.send(Ok(event)).is_ok() {
					DeliveryResult::Delivered
				} else {
					DeliveryResult::Disconnected
				}
			}
			Self::Batch(_) => DeliveryResult::Disconnected,
		}
	}

	fn send_batch_subscribed(&self, batch_id: BatchId, members: &[BatchSubscribedMember]) -> DeliveryResult {
		match self {
			Self::Batch(tx) => {
				let members_wire: Vec<BatchMember> = members
					.iter()
					.map(|m| BatchMember {
						index: m.index as u32,
						subscription_id: m.subscription_id.to_string(),
					})
					.collect();
				let event = BatchSubscriptionEvent {
					event: Some(batch_subscription_event::Event::Subscribed(
						BatchSubscribedEvent {
							batch_id: batch_id.to_string(),
							members: members_wire,
						},
					)),
				};
				if tx.send(Ok(event)).is_ok() {
					DeliveryResult::Delivered
				} else {
					DeliveryResult::Disconnected
				}
			}
			Self::Single(_) => DeliveryResult::Disconnected,
		}
	}

	fn send_change(&self, _sub_id: SubscriptionId, columns: Columns, format: Self::Format) -> DeliveryResult {
		match self {
			Self::Single(tx) => {
				let event = encode_change_event(columns, format);
				if tx.send(Ok(event)).is_ok() {
					DeliveryResult::Delivered
				} else {
					DeliveryResult::Disconnected
				}
			}
			Self::Batch(_) => DeliveryResult::Disconnected,
		}
	}

	fn send_remote_change(
		&self,
		_sub_id: SubscriptionId,
		payload: RawChangePayload,
		format: Self::Format,
	) -> DeliveryResult {
		match self {
			Self::Single(tx) => {
				let proto_payload = match (format, payload) {
					(WireFormat::Rbcf, RawChangePayload::Rbcf(bytes)) => {
						change_event::Payload::Rbcf(bytes)
					}
					(WireFormat::Rbcf, other) => {
						let frames = other.into_frames();
						change_event::Payload::Rbcf(
							encode_frames(&frames, &EncodeOptions::fast())
								.unwrap_or_default(),
						)
					}
					(WireFormat::Proto, payload) => {
						let frames = payload.into_frames();
						change_event::Payload::Frames(FramesPayload {
							frames: frames_to_proto(frames),
						})
					}
				};
				let event = SubscriptionEvent {
					event: Some(subscription_event::Event::Change(ChangeEvent {
						payload: Some(proto_payload),
					})),
				};
				if tx.send(Ok(event)).is_ok() {
					DeliveryResult::Delivered
				} else {
					DeliveryResult::Disconnected
				}
			}
			Self::Batch(_) => DeliveryResult::Disconnected,
		}
	}

	fn send_batch_envelope(
		&self,
		batch_id: BatchId,
		format: Self::Format,
		entries: Vec<(SubscriptionId, Vec<Frame>)>,
	) -> DeliveryResult {
		match self {
			Self::Batch(tx) => {
				let proto_entries: Vec<BatchChangeEntry> = entries
					.into_iter()
					.map(|(sub_id, frames)| {
						let payload = encode_change_payload(frames, format);
						BatchChangeEntry {
							subscription_id: sub_id.to_string(),
							change: Some(ChangeEvent {
								payload: Some(payload),
							}),
						}
					})
					.collect();
				let event = BatchSubscriptionEvent {
					event: Some(batch_subscription_event::Event::Change(BatchChangeEvent {
						batch_id: batch_id.to_string(),
						entries: proto_entries,
					})),
				};
				if tx.send(Ok(event)).is_ok() {
					DeliveryResult::Delivered
				} else {
					DeliveryResult::Disconnected
				}
			}
			Self::Single(_) => DeliveryResult::Disconnected,
		}
	}

	fn send_batch_member_closed(&self, batch_id: BatchId, subscription_id: SubscriptionId) -> DeliveryResult {
		match self {
			Self::Batch(tx) => {
				let event = BatchSubscriptionEvent {
					event: Some(batch_subscription_event::Event::MemberClosed(
						BatchMemberClosedEvent {
							batch_id: batch_id.to_string(),
							subscription_id: subscription_id.to_string(),
						},
					)),
				};
				if tx.send(Ok(event)).is_ok() {
					DeliveryResult::Delivered
				} else {
					DeliveryResult::Disconnected
				}
			}
			Self::Single(_) => DeliveryResult::Disconnected,
		}
	}

	fn send_closed(&self, _sub_id: SubscriptionId) -> DeliveryResult {
		DeliveryResult::Delivered
	}
}

pub fn encode_change_event(columns: Columns, format: WireFormat) -> SubscriptionEvent {
	let payload = encode_change_payload(vec![Frame::from(columns)], format);
	SubscriptionEvent {
		event: Some(subscription_event::Event::Change(ChangeEvent {
			payload: Some(payload),
		})),
	}
}

pub fn encode_change_payload(frames: Vec<Frame>, format: WireFormat) -> change_event::Payload {
	match format {
		WireFormat::Rbcf => {
			let rbcf = encode_frames(&frames, &EncodeOptions::fast()).unwrap_or_default();
			change_event::Payload::Rbcf(rbcf)
		}
		WireFormat::Proto => change_event::Payload::Frames(FramesPayload {
			frames: frames_to_proto(frames),
		}),
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_sub_server::subscription::registry::PromoteResult;
	use reifydb_subscription::delivery::SubscriptionDelivery;
	use reifydb_value::value::{Value, uuid::Uuid7};

	use super::*;

	fn single_int_columns(name: &str, value: i64) -> Columns {
		Columns::single_row([(name, Value::Int8(value))])
	}

	fn test_clock_and_rng() -> (Clock, Rng) {
		let clock = Clock::Mock(MockClock::from_millis(1000));
		let rng = Rng::seeded(42);
		(clock, rng)
	}

	#[tokio::test]
	async fn test_batch_flush_coalesces_two_members() {
		let (clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();
		let batch_sink = GrpcWireSink::Batch(batch_tx);

		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);

		registry.subscribe(
			sub_a,
			connection_id,
			"FROM a".to_string(),
			batch_sink.clone(),
			WireFormat::Proto,
			None,
			Duration::ZERO,
		);
		registry.subscribe(
			sub_b,
			connection_id,
			"FROM b".to_string(),
			batch_sink.clone(),
			WireFormat::Proto,
			None,
			Duration::ZERO,
		);

		let batch_id = registry.register_batch(
			connection_id,
			vec![(sub_a, Duration::ZERO), (sub_b, Duration::ZERO)],
			batch_sink,
			WireFormat::Proto,
			&clock,
			&rng,
		);
		assert_eq!(registry.batch_for(&sub_a), Some(batch_id));

		assert!(matches!(registry.try_deliver(&sub_a, single_int_columns("v", 1)), DeliveryResult::Delivered));
		assert!(matches!(registry.try_deliver(&sub_b, single_int_columns("v", 2)), DeliveryResult::Delivered));

		assert!(batch_rx.try_recv().is_err());

		registry.flush();

		let msg = batch_rx.try_recv().expect("envelope").expect("ok");
		assert!(batch_rx.try_recv().is_err());
		match msg.event {
			Some(batch_subscription_event::Event::Change(c)) => {
				assert_eq!(c.batch_id, batch_id.to_string());
				assert_eq!(c.entries.len(), 2);
			}
			other => panic!("expected Change, got {:?}", other.is_some()),
		}
	}

	#[tokio::test]
	async fn test_push_batch_frames_for_remote_member() {
		let (clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();
		let batch_sink = GrpcWireSink::Batch(batch_tx);
		let sub_remote = SubscriptionId(42);

		let batch_id = registry.register_batch(
			connection_id,
			vec![(sub_remote, Duration::ZERO)],
			batch_sink,
			WireFormat::Proto,
			&clock,
			&rng,
		);

		let frames = vec![Frame::from(single_int_columns("v", 99))];
		assert!(registry.push_batch_frames(batch_id, sub_remote, frames));

		registry.flush();
		let msg = batch_rx.try_recv().expect("envelope").expect("ok");
		match msg.event {
			Some(batch_subscription_event::Event::Change(c)) => {
				assert_eq!(c.entries.len(), 1);
				assert_eq!(c.entries[0].subscription_id, sub_remote.to_string());
			}
			_ => panic!("expected Change"),
		}
	}

	#[tokio::test]
	async fn test_emit_batch_member_closed() {
		let (clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();
		let batch_sink = GrpcWireSink::Batch(batch_tx);
		let sub = SubscriptionId(123);

		let batch_id = registry.register_batch(
			connection_id,
			vec![(sub, Duration::ZERO)],
			batch_sink,
			WireFormat::Proto,
			&clock,
			&rng,
		);

		assert!(registry.emit_batch_member_closed(batch_id, sub));

		let msg = batch_rx.try_recv().expect("event").expect("ok");
		match msg.event {
			Some(batch_subscription_event::Event::MemberClosed(m)) => {
				assert_eq!(m.batch_id, batch_id.to_string());
				assert_eq!(m.subscription_id, sub.to_string());
			}
			_ => panic!("expected MemberClosed"),
		}
	}

	#[tokio::test]
	async fn test_warming_buffers_until_promote() {
		let (clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx, mut rx) = mpsc::unbounded_channel::<Result<SubscriptionEvent, Status>>();
		let sink = GrpcWireSink::Single(tx);

		let sub = SubscriptionId(7001);
		registry.subscribe(
			sub,
			connection_id,
			"FROM warm".to_string(),
			sink,
			WireFormat::Proto,
			Some(16),
			Duration::ZERO,
		);

		assert!(matches!(registry.try_deliver(&sub, single_int_columns("v", 1)), DeliveryResult::Delivered));
		assert!(matches!(registry.try_deliver(&sub, single_int_columns("v", 2)), DeliveryResult::Delivered));
		assert!(rx.try_recv().is_err(), "no pushes while warming");

		match registry.promote_to_live(sub) {
			PromoteResult::Promoted(n) => assert_eq!(n, 2),
			other => panic!("unexpected promote result: {:?}", other),
		}

		let first = rx.try_recv().expect("expected first buffered push after promote").expect("ok");
		assert!(matches!(first.event, Some(subscription_event::Event::Change(_))));
		let second = rx.try_recv().expect("expected second buffered push after promote").expect("ok");
		assert!(matches!(second.event, Some(subscription_event::Event::Change(_))));

		assert!(matches!(registry.try_deliver(&sub, single_int_columns("v", 3)), DeliveryResult::Delivered));
		let live = rx.try_recv().expect("expected live push after promote").expect("ok");
		assert!(matches!(live.event, Some(subscription_event::Event::Change(_))));
	}

	#[tokio::test]
	async fn test_warming_overflow_marks_subscription() {
		let (clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx, _rx) = mpsc::unbounded_channel::<Result<SubscriptionEvent, Status>>();
		let sink = GrpcWireSink::Single(tx);

		let sub = SubscriptionId(7002);
		registry.subscribe(
			sub,
			connection_id,
			"FROM warm".to_string(),
			sink,
			WireFormat::Proto,
			Some(2),
			Duration::ZERO,
		);

		registry.try_deliver(&sub, single_int_columns("v", 1));
		registry.try_deliver(&sub, single_int_columns("v", 2));
		registry.try_deliver(&sub, single_int_columns("v", 3));

		match registry.promote_to_live(sub) {
			PromoteResult::Overflowed => {}
			other => panic!("expected Overflowed, got {:?}", other),
		}
	}

	#[test]
	fn test_batch_id_display_fromstr_roundtrip() {
		let (clock, rng) = test_clock_and_rng();
		let id = BatchId(Uuid7::generate(&clock, &rng));
		let rendered = id.to_string();
		let parsed: BatchId = rendered.parse().expect("parse roundtrip");
		assert_eq!(id, parsed);
	}
}
