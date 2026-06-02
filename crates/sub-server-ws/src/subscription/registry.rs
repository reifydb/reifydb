// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_client::{RawChangePayload, WireFormat as ClientWireFormat};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_sub_server::{
	format::WireFormat,
	response::{CONTENT_TYPE_FRAMES, CONTENT_TYPE_JSON, resolve_response_json},
	subscription::wire_sink::{BatchSubscribedMember, WireSink},
};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_value::value::{frame::frame::Frame, uuid::Uuid7};
use reifydb_wire_format::{encode::encode_frames, json::to::convert_frames, options::EncodeOptions};
use serde_json::{Value as JsonValue, from_str, json};
use tokio::sync::mpsc;
use tracing::warn;

use crate::handler::{BinaryKind, encode_rbcf_envelope};

pub type ConnectionId = Uuid7;

pub type SubscriptionRegistry = reifydb_sub_server::subscription::registry::SubscriptionRegistry<WsWireSink>;

#[derive(Debug, Clone)]
pub struct BatchChangeEntryPush {
	pub subscription_id: SubscriptionId,
	pub content_type: String,
	pub body: JsonValue,
}

#[derive(Debug, Clone)]
pub enum PushMessage {
	ChangeJson {
		subscription_id: SubscriptionId,
		content_type: String,
		body: JsonValue,
	},

	ChangeRbcf {
		subscription_id: SubscriptionId,
		envelope: Vec<u8>,
	},

	Closed {
		subscription_id: SubscriptionId,
	},

	BatchChangeJson {
		batch_id: BatchId,
		entries: Vec<BatchChangeEntryPush>,
	},

	BatchChangeRbcf {
		batch_id: BatchId,
		envelope: Vec<u8>,
	},

	BatchMemberClosed {
		batch_id: BatchId,
		subscription_id: SubscriptionId,
	},

	BatchClosed {
		batch_id: BatchId,
	},
}

#[derive(Clone)]
pub struct WsWireSink {
	pub push_tx: mpsc::UnboundedSender<PushMessage>,
}

impl WsWireSink {
	pub fn new(push_tx: mpsc::UnboundedSender<PushMessage>) -> Self {
		Self {
			push_tx,
		}
	}
}

impl WireSink for WsWireSink {
	type Format = WireFormat;

	fn client_wire_format(format: Self::Format) -> ClientWireFormat {
		match format {
			WireFormat::Rbcf => ClientWireFormat::Rbcf,
			WireFormat::Json | WireFormat::Frames => ClientWireFormat::Rbcf,
		}
	}

	fn send_subscribed(&self, _sub_id: SubscriptionId) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_batch_subscribed(&self, _batch_id: BatchId, _members: &[BatchSubscribedMember]) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_change(&self, sub_id: SubscriptionId, columns: Columns, format: Self::Format) -> DeliveryResult {
		let msg = match encode_change(sub_id, columns, format) {
			Some(m) => m,
			None => return DeliveryResult::Disconnected,
		};
		if self.push_tx.send(msg).is_ok() {
			DeliveryResult::Delivered
		} else {
			DeliveryResult::Disconnected
		}
	}

	fn send_remote_change(
		&self,
		sub_id: SubscriptionId,
		payload: RawChangePayload,
		format: Self::Format,
	) -> DeliveryResult {
		let msg = match (format, payload) {
			(WireFormat::Rbcf, RawChangePayload::Rbcf(bytes)) => {
				let envelope =
					encode_rbcf_envelope(BinaryKind::Change, &sub_id.to_string(), &bytes, None);
				PushMessage::ChangeRbcf {
					subscription_id: sub_id,
					envelope,
				}
			}
			(WireFormat::Rbcf, other) => {
				let frames = other.into_frames();
				let rbcf_bytes = match encode_frames(&frames, &EncodeOptions::fast()) {
					Ok(b) => b,
					Err(e) => {
						warn!("Failed to RBCF-encode remote change for {}: {}", sub_id, e);
						return DeliveryResult::Disconnected;
					}
				};
				let envelope = encode_rbcf_envelope(
					BinaryKind::Change,
					&sub_id.to_string(),
					&rbcf_bytes,
					None,
				);
				PushMessage::ChangeRbcf {
					subscription_id: sub_id,
					envelope,
				}
			}
			(WireFormat::Frames, payload) => {
				let frames = payload.into_frames();
				PushMessage::ChangeJson {
					subscription_id: sub_id,
					content_type: CONTENT_TYPE_FRAMES.to_string(),
					body: json!({ "frames": convert_frames(&frames) }),
				}
			}
			(WireFormat::Json, payload) => {
				let frames = payload.into_frames();
				let body = match resolve_response_json(frames, false) {
					Ok(r) => from_str::<JsonValue>(&r.body).unwrap_or(JsonValue::String(r.body)),
					Err(_) => JsonValue::Array(vec![]),
				};
				PushMessage::ChangeJson {
					subscription_id: sub_id,
					content_type: CONTENT_TYPE_JSON.to_string(),
					body,
				}
			}
		};
		if self.push_tx.send(msg).is_ok() {
			DeliveryResult::Delivered
		} else {
			DeliveryResult::Disconnected
		}
	}

	fn send_batch_envelope(
		&self,
		batch_id: BatchId,
		format: Self::Format,
		entries: Vec<(SubscriptionId, Vec<Frame>)>,
	) -> DeliveryResult {
		let msg = match format {
			WireFormat::Rbcf => {
				let mut rbcf_entries: Vec<(String, Vec<u8>)> = Vec::with_capacity(entries.len());
				for (sub_id, frames) in entries {
					let rbcf_bytes = match encode_frames(&frames, &EncodeOptions::fast()) {
						Ok(b) => b,
						Err(e) => {
							warn!(
								"Failed to RBCF-encode batch entry for {}/{}: {}",
								batch_id, sub_id, e
							);
							continue;
						}
					};
					rbcf_entries.push((sub_id.to_string(), rbcf_bytes));
				}
				if rbcf_entries.is_empty() {
					return DeliveryResult::Delivered;
				}
				let envelope = encode_rbcf_batch_envelope(&batch_id.to_string(), &rbcf_entries);
				PushMessage::BatchChangeRbcf {
					batch_id,
					envelope,
				}
			}
			WireFormat::Frames => {
				let json_entries = entries
					.into_iter()
					.map(|(sub_id, frames)| {
						let body = json!({ "frames": convert_frames(&frames) });
						BatchChangeEntryPush {
							subscription_id: sub_id,
							content_type: CONTENT_TYPE_FRAMES.to_string(),
							body,
						}
					})
					.collect();
				PushMessage::BatchChangeJson {
					batch_id,
					entries: json_entries,
				}
			}
			WireFormat::Json => {
				let json_entries: Vec<BatchChangeEntryPush> = entries
					.into_iter()
					.filter_map(|(sub_id, frames)| {
						let resolved = match resolve_response_json(frames, false) {
							Ok(r) => r,
							Err(e) => {
								warn!(
									"Failed to JSON-encode batch entry for {}/{}: {}",
									batch_id, sub_id, e
								);
								return None;
							}
						};
						let body = from_str(&resolved.body)
							.unwrap_or(JsonValue::String(resolved.body));
						Some(BatchChangeEntryPush {
							subscription_id: sub_id,
							content_type: CONTENT_TYPE_JSON.to_string(),
							body,
						})
					})
					.collect();
				if json_entries.is_empty() {
					return DeliveryResult::Delivered;
				}
				PushMessage::BatchChangeJson {
					batch_id,
					entries: json_entries,
				}
			}
		};

		if self.push_tx.send(msg).is_ok() {
			DeliveryResult::Delivered
		} else {
			DeliveryResult::Disconnected
		}
	}

	fn send_batch_member_closed(&self, batch_id: BatchId, subscription_id: SubscriptionId) -> DeliveryResult {
		if self.push_tx
			.send(PushMessage::BatchMemberClosed {
				batch_id,
				subscription_id,
			})
			.is_ok()
		{
			DeliveryResult::Delivered
		} else {
			DeliveryResult::Disconnected
		}
	}

	fn send_closed(&self, subscription_id: SubscriptionId) -> DeliveryResult {
		if self.push_tx
			.send(PushMessage::Closed {
				subscription_id,
			})
			.is_ok()
		{
			DeliveryResult::Delivered
		} else {
			DeliveryResult::Disconnected
		}
	}
}

pub fn encode_change_for_handler(
	subscription_id: SubscriptionId,
	columns: Columns,
	format: WireFormat,
) -> Option<PushMessage> {
	encode_change(subscription_id, columns, format)
}

fn encode_change(subscription_id: SubscriptionId, columns: Columns, format: WireFormat) -> Option<PushMessage> {
	match format {
		WireFormat::Rbcf => {
			let frames = vec![Frame::from(columns)];
			let rbcf_bytes = match encode_frames(&frames, &EncodeOptions::fast()) {
				Ok(b) => b,
				Err(e) => {
					warn!("Failed to RBCF-encode change for {}: {}", subscription_id, e);
					return None;
				}
			};
			let envelope = encode_rbcf_envelope(
				BinaryKind::Change,
				&subscription_id.to_string(),
				&rbcf_bytes,
				None,
			);
			Some(PushMessage::ChangeRbcf {
				subscription_id,
				envelope,
			})
		}
		WireFormat::Frames => {
			let body = json!({ "frames": convert_frames(&[Frame::from(columns)]) });
			Some(PushMessage::ChangeJson {
				subscription_id,
				content_type: CONTENT_TYPE_FRAMES.to_string(),
				body,
			})
		}
		WireFormat::Json => {
			let frames = vec![Frame::from(columns)];
			let resolved = match resolve_response_json(frames, false) {
				Ok(r) => r,
				Err(e) => {
					warn!("Failed to JSON-encode change for {}: {}", subscription_id, e);
					return None;
				}
			};
			let body = from_str(&resolved.body).unwrap_or(JsonValue::String(resolved.body));
			Some(PushMessage::ChangeJson {
				subscription_id,
				content_type: CONTENT_TYPE_JSON.to_string(),
				body,
			})
		}
	}
}

fn encode_rbcf_batch_envelope(batch_id: &str, entries: &[(String, Vec<u8>)]) -> Vec<u8> {
	let batch_id_bytes = batch_id.as_bytes();
	let mut total_entries_bytes = 0usize;
	for (sub_id, rbcf) in entries {
		total_entries_bytes += 4 + sub_id.len() + 4 + rbcf.len();
	}

	let mut envelope = Vec::with_capacity(1 + 4 + batch_id_bytes.len() + 4 + total_entries_bytes);
	envelope.push(BinaryKind::BatchChange as u8);
	envelope.extend_from_slice(&(batch_id_bytes.len() as u32).to_le_bytes());
	envelope.extend_from_slice(batch_id_bytes);
	envelope.extend_from_slice(&(entries.len() as u32).to_le_bytes());

	for (sub_id, rbcf) in entries {
		let sub_id_bytes = sub_id.as_bytes();
		envelope.extend_from_slice(&(sub_id_bytes.len() as u32).to_le_bytes());
		envelope.extend_from_slice(sub_id_bytes);
		envelope.extend_from_slice(&(rbcf.len() as u32).to_le_bytes());
		envelope.extend_from_slice(rbcf);
	}
	envelope
}

#[cfg(test)]
pub mod tests {
	use std::{collections::HashSet, time::Duration};

	use reifydb_core::interface::catalog::id::SubscriptionId;
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_sub_server::subscription::registry::PromoteResult;
	use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
	use reifydb_value::value::{Value, uuid::Uuid7};

	use super::*;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	fn single_int_columns(name: &str, value: i64) -> Columns {
		Columns::single_row([(name, Value::Int8(value))])
	}

	#[tokio::test]
	async fn test_subscribe_unsubscribe() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx, _rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(tx);

		let sub_id = SubscriptionId(12345);
		registry.subscribe(
			sub_id,
			connection_id,
			"FROM test".to_string(),
			sink,
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		assert_eq!(registry.subscription_count(), 1);

		assert!(registry.unsubscribe(sub_id));
		assert_eq!(registry.subscription_count(), 0);
		assert_eq!(registry.connection_count(), 0);

		assert!(!registry.unsubscribe(sub_id));
	}

	#[tokio::test]
	async fn test_cleanup_connection() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (tx1, _rx1) = mpsc::unbounded_channel();
		let (tx2, _rx2) = mpsc::unbounded_channel();

		let sub1 = SubscriptionId(12345);
		let sub2 = SubscriptionId(12346);
		registry.subscribe(
			sub1,
			connection_id,
			"FROM test1".to_string(),
			WsWireSink::new(tx1),
			WireFormat::Json,
			None,
			Duration::ZERO,
		);
		registry.subscribe(
			sub2,
			connection_id,
			"FROM test2".to_string(),
			WsWireSink::new(tx2),
			WireFormat::Json,
			None,
			Duration::ZERO,
		);
		assert_eq!(registry.subscription_count(), 2);

		registry.cleanup_connection(connection_id);
		assert_eq!(registry.subscription_count(), 0);
		assert_eq!(registry.connection_count(), 0);
	}

	#[tokio::test]
	async fn test_batch_flush_coalesces_two_members() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);

		registry.subscribe(
			sub_a,
			connection_id,
			"FROM a".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		registry.subscribe(
			sub_b,
			connection_id,
			"FROM b".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);

		let batch_id = registry.register_batch(
			connection_id,
			vec![(sub_a, Duration::ZERO), (sub_b, Duration::ZERO)],
			sink.clone(),
			WireFormat::Frames,
			&clock,
			&rng,
		);
		assert_eq!(registry.batch_count(), 1);
		assert_eq!(registry.batch_for(&sub_a), Some(batch_id));
		assert_eq!(registry.batch_for(&sub_b), Some(batch_id));

		assert!(matches!(
			registry.try_deliver(&sub_a, single_int_columns("value", 10)),
			DeliveryResult::Delivered
		));
		assert!(matches!(
			registry.try_deliver(&sub_b, single_int_columns("value", 20)),
			DeliveryResult::Delivered
		));

		assert!(push_rx.try_recv().is_err());

		registry.flush();

		let msg = push_rx.try_recv().expect("expected one BatchChange after flush");
		assert!(push_rx.try_recv().is_err());
		match msg {
			PushMessage::BatchChangeJson {
				batch_id: bid,
				entries,
			} => {
				assert_eq!(bid, batch_id);
				assert_eq!(entries.len(), 2);
				let ids: HashSet<SubscriptionId> = entries.iter().map(|e| e.subscription_id).collect();
				assert!(ids.contains(&sub_a));
				assert!(ids.contains(&sub_b));
			}
			other => panic!("expected BatchChangeJson, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_batch_flush_merges_repeated_member_deliveries() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub_a = SubscriptionId(100);
		registry.subscribe(
			sub_a,
			connection_id,
			"FROM a".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		let batch_id = registry.register_batch(
			connection_id,
			vec![(sub_a, Duration::ZERO)],
			sink,
			WireFormat::Frames,
			&clock,
			&rng,
		);

		registry.try_deliver(&sub_a, single_int_columns("value", 1));
		registry.try_deliver(&sub_a, single_int_columns("value", 2));

		registry.flush();

		let msg = push_rx.try_recv().expect("envelope");
		match msg {
			PushMessage::BatchChangeJson {
				batch_id: bid,
				entries,
			} => {
				assert_eq!(bid, batch_id);
				assert_eq!(entries.len(), 1);
				assert_eq!(entries[0].subscription_id, sub_a);
				let frames = entries[0].body.get("frames").expect("frames key").as_array().unwrap();
				assert_eq!(frames.len(), 2);
			}
			other => panic!("expected BatchChangeJson, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_warming_buffers_until_promote() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub = SubscriptionId(7001);
		registry.subscribe(
			sub,
			connection_id,
			"FROM warm".to_string(),
			sink,
			WireFormat::Frames,
			Some(16),
			Duration::ZERO,
		);

		assert!(matches!(registry.try_deliver(&sub, single_int_columns("v", 1)), DeliveryResult::Delivered));
		assert!(matches!(registry.try_deliver(&sub, single_int_columns("v", 2)), DeliveryResult::Delivered));
		assert!(push_rx.try_recv().is_err(), "no pushes while warming");

		match registry.promote_to_live(sub) {
			PromoteResult::Promoted(n) => assert_eq!(n, 2),
			other => panic!("unexpected promote result: {:?}", other),
		}

		let msg = push_rx.try_recv().expect("expected first buffered push after promote");
		assert!(matches!(msg, PushMessage::ChangeJson { subscription_id, .. } if subscription_id == sub));
		let msg = push_rx.try_recv().expect("expected second buffered push after promote");
		assert!(matches!(msg, PushMessage::ChangeJson { subscription_id, .. } if subscription_id == sub));

		assert!(matches!(registry.try_deliver(&sub, single_int_columns("v", 3)), DeliveryResult::Delivered));
		let msg = push_rx.try_recv().expect("expected live push after promote");
		assert!(matches!(msg, PushMessage::ChangeJson { subscription_id, .. } if subscription_id == sub));
	}

	#[tokio::test]
	async fn test_warming_overflow_marks_subscription() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, _push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub = SubscriptionId(7002);
		registry.subscribe(
			sub,
			connection_id,
			"FROM warm".to_string(),
			sink,
			WireFormat::Frames,
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

	#[tokio::test]
	async fn test_promote_unknown_subscription() {
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(Clock::Mock(MockClock::from_millis(0)));
		match registry.promote_to_live(SubscriptionId(999)) {
			PromoteResult::NotFound => {}
			other => panic!("expected NotFound, got {:?}", other),
		}
	}

	#[test]
	fn test_batch_id_display_fromstr_roundtrip() {
		let (_, clock, rng) = test_clock_and_rng();
		let id = BatchId(Uuid7::generate(&clock, &rng));
		let rendered = id.to_string();
		let parsed: BatchId = rendered.parse().expect("parse roundtrip");
		assert_eq!(id, parsed);
	}

	#[tokio::test]
	async fn test_linger_respects_each_member_independently_not_max() {
		let (mock, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub_short = SubscriptionId(1);
		let sub_long = SubscriptionId(2);
		registry.subscribe(
			sub_short,
			connection_id,
			"FROM a".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		registry.subscribe(
			sub_long,
			connection_id,
			"FROM b".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		registry.register_batch(
			connection_id,
			vec![(sub_short, Duration::from_millis(5)), (sub_long, Duration::from_millis(50))],
			sink,
			WireFormat::Frames,
			&clock,
			&rng,
		);

		registry.try_deliver(&sub_short, single_int_columns("v", 1));
		registry.try_deliver(&sub_long, single_int_columns("v", 2));

		registry.flush();
		assert!(push_rx.try_recv().is_err(), "no member is due before its own linger elapses");

		mock.advance_millis(5);
		registry.flush();
		match push_rx.try_recv().expect("the short-linger member is due") {
			PushMessage::BatchChangeJson {
				entries,
				..
			} => {
				assert_eq!(
					entries.len(),
					1,
					"only the short-linger member flushes - lingers are respected individually, never max-ed across the batch"
				);
				assert_eq!(entries[0].subscription_id, sub_short);
			}
			other => panic!("expected BatchChangeJson, got {:?}", other),
		}
		assert!(
			push_rx.try_recv().is_err(),
			"the long-linger member is held back, not dragged out early by the short one"
		);

		mock.advance_millis(50);
		registry.flush();
		match push_rx.try_recv().expect("the long-linger member is now due") {
			PushMessage::BatchChangeJson {
				entries,
				..
			} => {
				assert_eq!(entries.len(), 1);
				assert_eq!(entries[0].subscription_id, sub_long);
			}
			other => panic!("expected BatchChangeJson, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_linger_concatenates_changes_within_its_window() {
		let (mock, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub_a = SubscriptionId(7);
		registry.subscribe(
			sub_a,
			connection_id,
			"FROM a".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		registry.register_batch(
			connection_id,
			vec![(sub_a, Duration::from_millis(10))],
			sink,
			WireFormat::Frames,
			&clock,
			&rng,
		);

		registry.try_deliver(&sub_a, single_int_columns("v", 1));
		mock.advance_millis(5);
		registry.try_deliver(&sub_a, single_int_columns("v", 2));

		registry.flush();
		assert!(
			push_rx.try_recv().is_err(),
			"the window anchors at the first pending change and is not reset by a later one"
		);

		mock.advance_millis(5);
		registry.flush();
		match push_rx.try_recv().expect("envelope after the window elapses") {
			PushMessage::BatchChangeJson {
				entries,
				..
			} => {
				assert_eq!(entries.len(), 1);
				let frames = entries[0].body.get("frames").expect("frames key").as_array().unwrap();
				assert_eq!(
					frames.len(),
					2,
					"both changes inside the linger window are concatenated into one frame-list"
				);
			}
			other => panic!("expected BatchChangeJson, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_linger_zero_flushes_on_the_next_flush() {
		let (_, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, mut push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub_z = SubscriptionId(9);
		registry.subscribe(
			sub_z,
			connection_id,
			"FROM z".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		registry.register_batch(
			connection_id,
			vec![(sub_z, Duration::ZERO)],
			sink,
			WireFormat::Frames,
			&clock,
			&rng,
		);

		registry.try_deliver(&sub_z, single_int_columns("v", 1));
		registry.flush();
		match push_rx.try_recv().expect("a zero-linger member is due immediately, with no added latency") {
			PushMessage::BatchChangeJson {
				entries,
				..
			} => assert_eq!(entries.len(), 1),
			other => panic!("expected BatchChangeJson, got {:?}", other),
		}
	}

	#[tokio::test]
	async fn test_linger_next_deadline_tracks_the_nearest_member() {
		let (mock, clock, rng) = test_clock_and_rng();
		let registry: SubscriptionRegistry = SubscriptionRegistry::new(clock.clone());
		let connection_id = Uuid7::generate(&clock, &rng);
		let (push_tx, _push_rx) = mpsc::unbounded_channel();
		let sink = WsWireSink::new(push_tx);

		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);
		registry.subscribe(
			sub_a,
			connection_id,
			"FROM a".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		registry.subscribe(
			sub_b,
			connection_id,
			"FROM b".to_string(),
			sink.clone(),
			WireFormat::Frames,
			None,
			Duration::ZERO,
		);
		registry.register_batch(
			connection_id,
			vec![(sub_a, Duration::from_millis(5)), (sub_b, Duration::from_millis(50))],
			sink,
			WireFormat::Frames,
			&clock,
			&rng,
		);

		registry.try_deliver(&sub_a, single_int_columns("v", 1));
		registry.try_deliver(&sub_b, single_int_columns("v", 2));

		assert_eq!(
			registry.flush(),
			Some(Duration::from_millis(5)),
			"the poller is told to wake at the soonest member deadline, so a low-linger member never starves"
		);

		mock.advance_millis(5);
		assert_eq!(
			registry.flush(),
			Some(Duration::from_millis(45)),
			"after the near member drains, the deadline tracks the remaining member"
		);
	}
}
