// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{mem, time::Duration};

use dashmap::DashMap;
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_runtime::context::{clock::Clock, rng::Rng};
use reifydb_subscription::{
	batch::BatchId,
	delivery::{DeliveryResult, SubscriptionDelivery},
};
use reifydb_type::value::{frame::frame::Frame, uuid::Uuid7};
use reifydb_wire_format::{encode::encode_frames, options::EncodeOptions};
use tokio::sync::mpsc;
use tonic::Status;
use tracing::{debug, warn};

use crate::{
	convert::frames_to_proto,
	generated::{
		BatchChangeEntry, BatchChangeEvent, BatchMemberClosedEvent, BatchSubscriptionEvent, ChangeEvent,
		Format, FramesPayload, SubscriptionEvent, batch_subscription_event, change_event, subscription_event,
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

struct SubscriptionState {
	tx: Option<mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>>,
	format: WireFormat,
	batch_id: Option<BatchId>,
	throttle: ThrottleState,
}

struct ThrottleState {
	interval_millis: u64,
	last_sent_at: Option<u64>,
	pending: Vec<Columns>,
}

impl ThrottleState {
	fn new(interval: Duration) -> Self {
		Self {
			interval_millis: interval.as_millis() as u64,
			last_sent_at: None,
			pending: Vec::new(),
		}
	}

	fn enabled(&self) -> bool {
		self.interval_millis != 0
	}

	fn ready(&self, now_millis: u64) -> bool {
		match self.last_sent_at {
			None => true,
			Some(prev) => now_millis.saturating_sub(prev) >= self.interval_millis,
		}
	}
}

struct BatchState {
	tx: mpsc::UnboundedSender<Result<BatchSubscriptionEvent, Status>>,
	format: WireFormat,
	member_ids: Vec<SubscriptionId>,

	pending: DashMap<SubscriptionId, Vec<Frame>>,
}

type ThrottleReady =
	(SubscriptionId, Vec<Columns>, WireFormat, mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>);

pub struct GrpcSubscriptionRegistry {
	subscriptions: DashMap<SubscriptionId, SubscriptionState>,
	batches: DashMap<BatchId, BatchState>,
	clock: Clock,
}

impl GrpcSubscriptionRegistry {
	pub fn new(clock: Clock) -> Self {
		Self {
			subscriptions: DashMap::new(),
			batches: DashMap::new(),
			clock,
		}
	}

	pub fn register(
		&self,
		subscription_id: SubscriptionId,
		tx: mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>,
		format: WireFormat,
		throttle: Duration,
	) {
		self.subscriptions.insert(
			subscription_id,
			SubscriptionState {
				tx: Some(tx),
				format,
				batch_id: None,
				throttle: ThrottleState::new(throttle),
			},
		);
		debug!("Registered gRPC subscription {} (format={:?})", subscription_id, format);
	}

	pub fn register_batch_member(&self, subscription_id: SubscriptionId, format: WireFormat, throttle: Duration) {
		self.subscriptions.insert(
			subscription_id,
			SubscriptionState {
				tx: None,
				format,
				batch_id: None,
				throttle: ThrottleState::new(throttle),
			},
		);
		debug!("Registered gRPC batch member {} (format={:?})", subscription_id, format);
	}

	pub fn unregister(&self, subscription_id: &SubscriptionId) {
		self.subscriptions.remove(subscription_id);
		debug!("Unregistered gRPC subscription {}", subscription_id);
	}

	pub fn close_all(&self) {
		self.subscriptions.clear();
		self.batches.clear();
	}

	pub fn register_batch(
		&self,
		member_ids: Vec<SubscriptionId>,
		tx: mpsc::UnboundedSender<Result<BatchSubscriptionEvent, Status>>,
		format: WireFormat,
		clock: &Clock,
		rng: &Rng,
	) -> BatchId {
		let batch_id = BatchId(Uuid7::generate(clock, rng));
		for member_id in &member_ids {
			if let Some(mut state) = self.subscriptions.get_mut(member_id) {
				state.batch_id = Some(batch_id);
			}
		}
		self.batches.insert(
			batch_id,
			BatchState {
				tx,
				format,
				member_ids: member_ids.clone(),
				pending: DashMap::new(),
			},
		);
		debug!("Registered gRPC batch {} with {} members (format={:?})", batch_id, member_ids.len(), format);
		batch_id
	}

	pub fn unsubscribe_batch(&self, batch_id: BatchId) -> Option<Vec<SubscriptionId>> {
		let (_, state) = self.batches.remove(&batch_id)?;
		let members = state.member_ids.clone();
		for member_id in &members {
			self.subscriptions.remove(member_id);
		}
		debug!("Unregistered gRPC batch {} ({} members)", batch_id, members.len());
		Some(members)
	}

	pub fn batch_for(&self, subscription_id: &SubscriptionId) -> Option<BatchId> {
		self.subscriptions.get(subscription_id).and_then(|state| state.batch_id)
	}

	pub fn push_batch_frames(
		&self,
		batch_id: BatchId,
		subscription_id: SubscriptionId,
		frames: Vec<Frame>,
	) -> bool {
		let Some(batch) = self.batches.get(&batch_id) else {
			return false;
		};
		let mut entry = batch.pending.entry(subscription_id).or_default();
		for frame in frames {
			entry.push(frame);
		}
		true
	}

	pub fn emit_batch_member_closed(&self, batch_id: BatchId, subscription_id: SubscriptionId) -> bool {
		let Some(batch) = self.batches.get(&batch_id) else {
			return false;
		};
		let event = BatchSubscriptionEvent {
			event: Some(batch_subscription_event::Event::MemberClosed(BatchMemberClosedEvent {
				batch_id: batch_id.to_string(),
				subscription_id: subscription_id.to_string(),
			})),
		};
		batch.tx.send(Ok(event)).is_ok()
	}
}

impl SubscriptionDelivery for GrpcSubscriptionRegistry {
	fn try_deliver(&self, subscription_id: &SubscriptionId, columns: Columns) -> DeliveryResult {
		if let Some(batch_id) = self.batch_for(subscription_id) {
			if let Some(batch) = self.batches.get(&batch_id) {
				batch.pending.entry(*subscription_id).or_default().push(Frame::from(columns));
				return DeliveryResult::Delivered;
			}
			return DeliveryResult::Disconnected;
		}

		let mut state = match self.subscriptions.get_mut(subscription_id) {
			Some(s) => s,
			None => return DeliveryResult::Disconnected,
		};
		let tx = match state.tx.clone() {
			Some(tx) => tx,
			None => return DeliveryResult::Disconnected,
		};

		if state.throttle.enabled() {
			let now = self.clock.now_millis();
			if state.throttle.ready(now) && state.throttle.pending.is_empty() {
				let event = encode_change_event(columns, state.format);
				match tx.send(Ok(event)) {
					Ok(_) => {
						state.throttle.last_sent_at = Some(now);
						DeliveryResult::Delivered
					}
					Err(_) => DeliveryResult::Disconnected,
				}
			} else {
				state.throttle.pending.push(columns);
				DeliveryResult::Delivered
			}
		} else {
			let event = encode_change_event(columns, state.format);
			match tx.send(Ok(event)) {
				Ok(_) => DeliveryResult::Delivered,
				Err(_) => DeliveryResult::Disconnected,
			}
		}
	}

	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.subscriptions.iter().map(|entry| *entry.key()).collect()
	}

	fn flush(&self) {
		let now = self.clock.now_millis();
		let mut throttle_ready: Vec<ThrottleReady> = Vec::new();

		for mut entry in self.subscriptions.iter_mut() {
			let sub_id = *entry.key();
			let state = entry.value_mut();
			if state.batch_id.is_some() {
				continue;
			}
			if !state.throttle.enabled() || state.throttle.pending.is_empty() {
				continue;
			}
			if !state.throttle.ready(now) {
				continue;
			}
			let tx = match state.tx.clone() {
				Some(tx) => tx,
				None => continue,
			};
			let drained = mem::take(&mut state.throttle.pending);
			state.throttle.last_sent_at = Some(now);
			throttle_ready.push((sub_id, drained, state.format, tx));
		}

		let mut dead_subs: Vec<SubscriptionId> = Vec::new();
		for (sub_id, drained, format, tx) in throttle_ready {
			for columns in drained {
				let event = encode_change_event(columns, format);
				if tx.send(Ok(event)).is_err() {
					dead_subs.push(sub_id);
					break;
				}
			}
		}
		for sub_id in dead_subs {
			self.unregister(&sub_id);
		}

		let mut dead_batches: Vec<BatchId> = Vec::new();

		for entry in self.batches.iter() {
			let batch_id = *entry.key();
			let batch = entry.value();

			let taken: Vec<(SubscriptionId, Vec<Frame>)> = batch
				.pending
				.iter_mut()
				.filter_map(|mut e| {
					let v = mem::take(e.value_mut());
					if v.is_empty() {
						None
					} else {
						Some((*e.key(), v))
					}
				})
				.collect();
			if taken.is_empty() {
				continue;
			}

			let entries: Vec<BatchChangeEntry> = taken
				.into_iter()
				.map(|(sub_id, frames)| {
					let payload = encode_change_payload(frames, batch.format);
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
					entries,
				})),
			};

			if batch.tx.send(Ok(event)).is_err() {
				dead_batches.push(batch_id);
			}
		}

		for batch_id in dead_batches {
			if let Some(members) = self.unsubscribe_batch(batch_id) {
				warn!("gRPC batch {} tx closed; cascaded {} members", batch_id, members.len());
			}
		}
	}
}

fn encode_change_event(columns: Columns, format: WireFormat) -> SubscriptionEvent {
	let payload = encode_change_payload(vec![Frame::from(columns)], format);
	SubscriptionEvent {
		event: Some(subscription_event::Event::Change(ChangeEvent {
			payload: Some(payload),
		})),
	}
}

fn encode_change_payload(frames: Vec<Frame>, format: WireFormat) -> change_event::Payload {
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
	use reifydb_runtime::context::{clock::MockClock, rng::Rng};
	use reifydb_type::value::Value;

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
		let registry = GrpcSubscriptionRegistry::new(clock.clone());
		let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();

		let sub_a = SubscriptionId(1);
		let sub_b = SubscriptionId(2);

		registry.register_batch_member(sub_a, WireFormat::Proto, Duration::ZERO);
		registry.register_batch_member(sub_b, WireFormat::Proto, Duration::ZERO);

		let batch_id = registry.register_batch(vec![sub_a, sub_b], batch_tx, WireFormat::Proto, &clock, &rng);
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
	async fn test_batch_flush_empty_tick_is_noop() {
		let (clock, rng) = test_clock_and_rng();
		let registry = GrpcSubscriptionRegistry::new(clock.clone());
		let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();
		let sub_a = SubscriptionId(77);
		registry.register_batch_member(sub_a, WireFormat::Proto, Duration::ZERO);
		registry.register_batch(vec![sub_a], batch_tx, WireFormat::Proto, &clock, &rng);

		registry.flush();
		assert!(batch_rx.try_recv().is_err());
	}

	#[tokio::test]
	async fn test_push_batch_frames_for_remote_member() {
		let (clock, rng) = test_clock_and_rng();
		let registry = GrpcSubscriptionRegistry::new(clock.clone());
		let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();
		let sub_remote = SubscriptionId(42);
		// Remote member: not in subscriptions map.
		let batch_id = registry.register_batch(vec![sub_remote], batch_tx, WireFormat::Proto, &clock, &rng);

		// Simulate frames arriving from the remote proxy.
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
		let registry = GrpcSubscriptionRegistry::new(clock.clone());
		let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<Result<BatchSubscriptionEvent, Status>>();
		let sub = SubscriptionId(123);
		let batch_id = registry.register_batch(vec![sub], batch_tx, WireFormat::Proto, &clock, &rng);

		assert!(registry.emit_batch_member_closed(batch_id, sub));

		let msg = batch_rx.try_recv().expect("event").expect("ok");
		match msg.event {
			Some(batch_subscription_event::Event::MemberClosed(m)) => {
				assert_eq!(m.batch_id, batch_id.to_string());
				assert_eq!(m.subscription_id, sub.to_string());
			}
			_ => panic!("expected MemberClosed"),
		}

		// Batch still alive.
		assert_eq!(registry.batch_for(&SubscriptionId(999)), None);
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
