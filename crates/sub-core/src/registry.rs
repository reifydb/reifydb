// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, mem, sync::Arc};

use arrow_array::RecordBatch;
use dashmap::DashMap;
use reifydb_codec::wire::RawChangePayload;
use reifydb_core::interface::{catalog::id::SubscriptionId, change::StagedBatch};
use reifydb_runtime::{context::clock::Clock, sync::mutex::Mutex};
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use reifydb_value::{
	error::Diagnostic,
	value::{diff_type::DiffType, duration::Duration, frame::frame::Frame, uuid::Uuid7},
};
use tokio::sync::Notify;
use tracing::{debug, instrument, warn};

use crate::wire_sink::{ChangeDelivery, WireSink};

pub type ConnectionId = Uuid7;

type ChangeGroup<S> = (ConnectionId, <S as WireSink>::Format, Vec<(SubscriptionId, Vec<Frame>)>);

#[derive(Debug)]
pub enum PromoteResult {
	Promoted(usize),
	Overflowed,
	NotWarming,
	NotFound,
}

struct SubscriptionState<S: WireSink> {
	connection_id: ConnectionId,
	format: S::Format,
	remote: bool,
	warming: Option<WarmingBuffer>,
	pending: SubscriptionPending,
}

struct ConnectionState<S: WireSink> {
	sink: S,
	dead: bool,
	subscription_ids: Vec<SubscriptionId>,
}

struct FlushGate {
	throttle_millis: i64,
	linger_millis: i64,
	first_pending_at: Option<i64>,
	last_sent_at: Option<i64>,
}

impl FlushGate {
	fn new(throttle: Duration, linger: Duration) -> Self {
		Self {
			throttle_millis: throttle.to_std().as_millis() as i64,
			linger_millis: linger.to_std().as_millis() as i64,
			first_pending_at: None,
			last_sent_at: None,
		}
	}

	fn on_pending(&mut self, now: i64) {
		if self.first_pending_at.is_none() {
			self.first_pending_at = Some(now);
		}
	}

	fn throttle_ok(&self, now: i64) -> bool {
		match self.last_sent_at {
			None => true,
			Some(prev) => now.saturating_sub(prev) >= self.throttle_millis,
		}
	}

	fn linger_ok(&self, now: i64) -> bool {
		match self.first_pending_at {
			None => false,
			Some(first) => now.saturating_sub(first) >= self.linger_millis,
		}
	}

	fn ready(&self, now: i64) -> bool {
		self.linger_ok(now) && self.throttle_ok(now)
	}

	fn remaining_millis(&self, now: i64) -> i64 {
		let linger_left = match self.first_pending_at {
			None => i64::MAX,
			Some(first) => self.linger_millis.saturating_sub(now.saturating_sub(first)),
		};
		let throttle_left = match self.last_sent_at {
			None => 0,
			Some(prev) => self.throttle_millis.saturating_sub(now.saturating_sub(prev)),
		};
		linger_left.max(throttle_left)
	}

	fn on_flush(&mut self, now: i64) {
		self.first_pending_at = None;
		self.last_sent_at = Some(now);
	}
}

struct WarmingBuffer {
	buffered: Vec<StagedBatch>,
	cap: usize,
	overflowed: bool,
}

impl WarmingBuffer {
	fn new(cap: usize) -> Self {
		Self {
			buffered: Vec::new(),
			cap,
			overflowed: false,
		}
	}

	fn push(&mut self, batch: StagedBatch) {
		if self.overflowed {
			return;
		}
		if self.buffered.len() >= self.cap {
			self.overflowed = true;
			self.buffered.clear();
			return;
		}
		self.buffered.push(batch);
	}
}

struct SubscriptionPending {
	frames: Vec<Frame>,
	gate: FlushGate,
}

impl SubscriptionPending {
	fn new(linger: Duration, throttle: Duration) -> Self {
		Self {
			frames: Vec::new(),
			gate: FlushGate::new(throttle, linger),
		}
	}

	fn push(&mut self, frame: Frame, now: i64) {
		self.gate.on_pending(now);
		self.frames.push(frame);
	}

	fn ready(&self, now: i64) -> bool {
		!self.frames.is_empty() && self.gate.ready(now)
	}

	fn remaining_millis(&self, now: i64) -> i64 {
		self.gate.remaining_millis(now)
	}
}

pub struct SubscriptionRegistry<S: WireSink> {
	subscriptions: DashMap<SubscriptionId, SubscriptionState<S>>,
	connections: DashMap<ConnectionId, ConnectionState<S>>,
	clock: Clock,
	wakers: Mutex<Vec<Arc<Notify>>>,
}

impl<S: WireSink> SubscriptionRegistry<S> {
	pub fn new(clock: Clock) -> Self {
		Self {
			subscriptions: DashMap::new(),
			connections: DashMap::new(),
			clock,
			wakers: Mutex::new(Vec::new()),
		}
	}

	#[allow(clippy::too_many_arguments)]
	pub fn subscribe(
		&self,
		subscription_id: SubscriptionId,
		connection_id: ConnectionId,
		sink: S,
		format: S::Format,
		warming_cap: Option<usize>,
		throttle: Duration,
		linger: Duration,
	) {
		self.insert(
			subscription_id,
			connection_id,
			sink,
			SubscriptionState {
				connection_id,
				format,
				remote: false,
				warming: warming_cap.map(WarmingBuffer::new),
				pending: SubscriptionPending::new(linger, throttle),
			},
		);
		debug!(
			"Registered subscription {} for connection {} (warming_cap={:?})",
			subscription_id, connection_id, warming_cap
		);
	}

	pub fn subscribe_remote(
		&self,
		subscription_id: SubscriptionId,
		connection_id: ConnectionId,
		sink: S,
		format: S::Format,
	) {
		self.insert(
			subscription_id,
			connection_id,
			sink,
			SubscriptionState {
				connection_id,
				format,
				remote: true,
				warming: None,
				pending: SubscriptionPending::new(Duration::zero(), Duration::zero()),
			},
		);
		debug!("Registered remote subscription {} for connection {}", subscription_id, connection_id);
	}

	#[inline]
	fn insert(
		&self,
		subscription_id: SubscriptionId,
		connection_id: ConnectionId,
		sink: S,
		state: SubscriptionState<S>,
	) {
		self.subscriptions.insert(subscription_id, state);
		self.connections
			.entry(connection_id)
			.or_insert_with(|| ConnectionState {
				sink,
				dead: false,
				subscription_ids: Vec::new(),
			})
			.subscription_ids
			.push(subscription_id);
	}

	pub fn promote_to_live(&self, subscription_id: SubscriptionId) -> PromoteResult {
		let count = {
			let Some(mut state) = self.subscriptions.get_mut(&subscription_id) else {
				return PromoteResult::NotFound;
			};
			let Some(warming) = state.warming.take() else {
				return PromoteResult::NotWarming;
			};
			if warming.overflowed {
				return PromoteResult::Overflowed;
			}
			let now = self.clock.now().to_millis();
			let count = warming.buffered.len();
			for (op, columns) in warming.buffered {
				state.pending.push(Frame::from(columns).with_op(op), now);
			}
			count
		};
		self.wake();
		PromoteResult::Promoted(count)
	}

	pub fn push_frames(&self, subscription_id: SubscriptionId, frames: Vec<Frame>) -> bool {
		{
			let Some(mut state) = self.subscriptions.get_mut(&subscription_id) else {
				return false;
			};
			let now = self.clock.now().to_millis();
			for frame in frames {
				state.pending.push(frame, now);
			}
		}
		self.wake();
		true
	}

	pub fn push_payload(&self, subscription_id: SubscriptionId, payload: RawChangePayload) -> bool {
		match payload.into_frames() {
			Ok(frames) => self.push_frames(subscription_id, frames),
			Err(e) => {
				warn!("Failed to decode remote change for {}, closing it: {}", subscription_id, e);
				false
			}
		}
	}

	pub fn close(&self, subscription_id: SubscriptionId) -> bool {
		self.end(subscription_id, true)
	}

	#[inline]
	fn end(&self, subscription_id: SubscriptionId, send_pending: bool) -> bool {
		let Some((_, state)) = self.subscriptions.remove(&subscription_id) else {
			return false;
		};
		let Some(sink) = self.detach(state.connection_id, subscription_id) else {
			return true;
		};
		let frames = state.pending.frames;
		if send_pending && !frames.is_empty() {
			let _ = sink.send_change(state.format, vec![(subscription_id, frames)]);
		}
		let _ = sink.send_subscription_closed(subscription_id);
		debug!("Closed subscription {}", subscription_id);
		true
	}

	#[inline]
	fn detach(&self, connection_id: ConnectionId, subscription_id: SubscriptionId) -> Option<S> {
		let (sink, emptied) = {
			let mut connection = self.connections.get_mut(&connection_id)?;
			connection.subscription_ids.retain(|id| *id != subscription_id);
			let sink = (!connection.dead).then(|| connection.sink.clone());
			(sink, connection.subscription_ids.is_empty())
		};
		if emptied {
			self.connections.remove(&connection_id);
		}
		sink
	}

	pub fn unsubscribe(&self, subscription_id: SubscriptionId) -> bool {
		let Some((_, state)) = self.subscriptions.remove(&subscription_id) else {
			return false;
		};
		let _ = self.detach(state.connection_id, subscription_id);
		debug!("Unsubscribed subscription {}", subscription_id);
		true
	}

	pub fn unsubscribe_owned(
		&self,
		connection_id: ConnectionId,
		subscription_ids: &[SubscriptionId],
	) -> Vec<SubscriptionId> {
		let mut removed = Vec::new();
		for subscription_id in subscription_ids {
			let remote = match self.subscriptions.get(subscription_id) {
				Some(state) if state.connection_id == connection_id => state.remote,
				_ => continue,
			};
			if self.unsubscribe(*subscription_id) && !remote {
				removed.push(*subscription_id);
			}
		}
		removed
	}

	pub fn connection_of(&self, subscription_id: &SubscriptionId) -> Option<ConnectionId> {
		self.subscriptions.get(subscription_id).map(|state| state.connection_id)
	}

	pub fn cleanup_connection(&self, connection_id: ConnectionId) -> Vec<SubscriptionId> {
		let Some((_, connection)) = self.connections.remove(&connection_id) else {
			return Vec::new();
		};
		let mut local = Vec::with_capacity(connection.subscription_ids.len());
		for subscription_id in connection.subscription_ids {
			if let Some((_, state)) = self.subscriptions.remove(&subscription_id)
				&& !state.remote
			{
				local.push(subscription_id);
			}
		}
		debug!("Cleaned up subscriptions for disconnected connection {}", connection_id);
		local
	}

	pub fn close_all(&self) {
		self.subscriptions.clear();
		self.connections.clear();
	}

	#[inline]
	fn wake(&self) {
		for waker in self.wakers.lock().iter() {
			waker.notify_one();
		}
	}

	#[inline]
	fn dead_connections(&self) -> HashSet<ConnectionId> {
		self.connections
			.iter()
			.filter(|connection| connection.dead)
			.map(|connection| *connection.key())
			.collect()
	}

	#[inline]
	fn flush_due(&self, now: i64, next_deadline: &mut Option<i64>) {
		let dead = self.dead_connections();
		let mut groups: Vec<ChangeGroup<S>> = Vec::new();
		for mut entry in self.subscriptions.iter_mut() {
			let subscription_id = *entry.key();
			let state = entry.value_mut();
			if state.warming.is_some() || state.pending.frames.is_empty() {
				continue;
			}
			if dead.contains(&state.connection_id) {
				state.pending.frames.clear();
				continue;
			}
			if !state.pending.ready(now) {
				let remaining = state.pending.remaining_millis(now);
				*next_deadline = Some(next_deadline.map_or(remaining, |d| d.min(remaining)));
				continue;
			}
			let frames = mem::take(&mut state.pending.frames);
			state.pending.gate.on_flush(now);
			match groups.iter_mut().find(|(connection_id, format, _)| {
				*connection_id == state.connection_id && *format == state.format
			}) {
				Some((_, _, entries)) => entries.push((subscription_id, frames)),
				None => groups.push((
					state.connection_id,
					state.format,
					vec![(subscription_id, frames)],
				)),
			}
		}

		for (connection_id, format, entries) in groups {
			let Some(sink) = self.connections.get(&connection_id).map(|connection| connection.sink.clone())
			else {
				continue;
			};
			match sink.send_change(format, entries) {
				ChangeDelivery::Delivered {
					failed,
				} => {
					for subscription_id in failed {
						self.end(subscription_id, false);
					}
				}
				ChangeDelivery::Disconnected => self.mark_dead(connection_id),
			}
		}
	}

	#[inline]
	fn mark_dead(&self, connection_id: ConnectionId) {
		let subscription_ids = {
			let Some(mut connection) = self.connections.get_mut(&connection_id) else {
				return;
			};
			connection.dead = true;
			connection.subscription_ids.clone()
		};
		for subscription_id in subscription_ids {
			if let Some(mut state) = self.subscriptions.get_mut(&subscription_id) {
				state.pending.frames.clear();
			}
		}
		debug!("Connection {} push channel closed; its subscriptions wait for cleanup", connection_id);
	}
}

impl<S: WireSink> SubscriptionDelivery for SubscriptionRegistry<S> {
	fn try_deliver(&self, subscription_id: &SubscriptionId, op: DiffType, columns: RecordBatch) -> DeliveryResult {
		let Some(mut state) = self.subscriptions.get_mut(subscription_id) else {
			return DeliveryResult::Disconnected;
		};
		if let Some(buffer) = state.warming.as_mut() {
			buffer.push((op, columns));
			return DeliveryResult::Delivered;
		}
		let now = self.clock.now().to_millis();
		state.pending.push(Frame::from(columns).with_op(op), now);
		DeliveryResult::Delivered
	}

	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		let mut out = Vec::new();
		self.active_subscriptions_into(&mut out);
		out
	}

	fn active_subscriptions_into(&self, out: &mut Vec<SubscriptionId>) {
		out.extend(self.subscriptions.iter().filter(|entry| !entry.remote).map(|entry| *entry.key()));
	}

	fn register_waker(&self, waker: Arc<Notify>) {
		self.wakers.lock().push(waker);
	}

	fn simulated_clock(&self) -> bool {
		self.clock.is_mock()
	}

	#[instrument(name = "server::flush", level = "debug", skip_all)]
	fn flush(&self) -> Option<Duration> {
		let now = self.clock.now().to_millis();
		let mut next_deadline: Option<i64> = None;
		self.flush_due(now, &mut next_deadline);
		next_deadline.map(|ms| Duration::from_milliseconds(ms).unwrap())
	}

	fn terminate(&self, subscription_id: &SubscriptionId, _diagnostic: Diagnostic) {
		self.end(*subscription_id, false);
	}
}
