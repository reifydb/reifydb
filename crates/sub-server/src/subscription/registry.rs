// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	mem,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration,
};

use dashmap::DashMap;
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_runtime::{
	context::{clock::Clock, rng::Rng},
	sync::mutex::Mutex,
};
use reifydb_subscription::{
	batch::BatchId,
	delivery::{DeliveryResult, SubscriptionDelivery},
};
use reifydb_value::value::{frame::frame::Frame, uuid::Uuid7};
use tokio::sync::Notify;
use tracing::{debug, info};

use crate::subscription::wire_sink::WireSink;

pub type ConnectionId = Uuid7;

#[derive(Debug)]
pub enum PromoteResult {
	Promoted(usize),
	Overflowed,
	NotWarming,
	NotFound,
	Disconnected,
}

struct SubscriptionState<S: WireSink> {
	connection_id: ConnectionId,
	sink: S,
	format: S::Format,
	#[allow(dead_code)]
	query: String,
	batch_id: Option<BatchId>,
	warming: Option<WarmingBuffer>,
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

	fn remaining_millis(&self, now_millis: u64) -> u64 {
		match self.last_sent_at {
			None => 0,
			Some(prev) => self.interval_millis.saturating_sub(now_millis.saturating_sub(prev)),
		}
	}
}

struct WarmingBuffer {
	buffered: Vec<Columns>,
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

	fn push(&mut self, columns: Columns) {
		if self.overflowed {
			return;
		}
		if self.buffered.len() >= self.cap {
			self.overflowed = true;
			self.buffered.clear();
			return;
		}
		self.buffered.push(columns);
	}
}

struct MemberPending {
	frames: Vec<Frame>,
	linger_millis: u64,
	first_pending_at: Option<u64>,
}

impl MemberPending {
	fn new(linger: Duration) -> Self {
		Self {
			frames: Vec::new(),
			linger_millis: linger.as_millis() as u64,
			first_pending_at: None,
		}
	}

	fn push(&mut self, frame: Frame, now: u64) {
		if self.frames.is_empty() {
			self.first_pending_at = Some(now);
		}
		self.frames.push(frame);
	}

	fn ready(&self, now: u64) -> bool {
		match self.first_pending_at {
			None => false,
			Some(first) => now.saturating_sub(first) >= self.linger_millis,
		}
	}

	fn remaining_millis(&self, now: u64) -> u64 {
		match self.first_pending_at {
			None => u64::MAX,
			Some(first) => self.linger_millis.saturating_sub(now.saturating_sub(first)),
		}
	}
}

struct BatchState<S: WireSink> {
	connection_id: ConnectionId,
	sink: S,
	format: S::Format,
	member_ids: Vec<SubscriptionId>,
	pending: DashMap<SubscriptionId, MemberPending>,
	lingers: HashMap<SubscriptionId, Duration>,
}

pub struct SubscriptionRegistry<S: WireSink> {
	subscriptions: DashMap<SubscriptionId, SubscriptionState<S>>,
	connections: DashMap<ConnectionId, Vec<SubscriptionId>>,
	batches: DashMap<BatchId, BatchState<S>>,
	connection_batches: DashMap<ConnectionId, Vec<BatchId>>,
	clock: Clock,
	throttle_pending: AtomicUsize,
	wakers: Mutex<Vec<Arc<Notify>>>,
}

impl<S: WireSink> SubscriptionRegistry<S> {
	pub fn new(clock: Clock) -> Self {
		Self {
			subscriptions: DashMap::new(),
			connections: DashMap::new(),
			batches: DashMap::new(),
			connection_batches: DashMap::new(),
			clock,
			throttle_pending: AtomicUsize::new(0),
			wakers: Mutex::new(Vec::new()),
		}
	}

	#[allow(clippy::too_many_arguments)]
	pub fn subscribe(
		&self,
		subscription_id: SubscriptionId,
		connection_id: ConnectionId,
		query: String,
		sink: S,
		format: S::Format,
		warming_cap: Option<usize>,
		throttle: Duration,
	) {
		self.subscriptions.insert(
			subscription_id,
			SubscriptionState {
				connection_id,
				sink,
				format,
				query,
				batch_id: None,
				warming: warming_cap.map(WarmingBuffer::new),
				throttle: ThrottleState::new(throttle),
			},
		);

		self.connections.entry(connection_id).or_default().push(subscription_id);

		debug!(
			"Registered subscription {} for connection {} (warming_cap={:?})",
			subscription_id, connection_id, warming_cap
		);
	}

	pub fn promote_to_live(&self, subscription_id: SubscriptionId) -> PromoteResult {
		let mut state = match self.subscriptions.get_mut(&subscription_id) {
			Some(s) => s,
			None => return PromoteResult::NotFound,
		};
		let warming = match state.warming.as_ref() {
			Some(w) => w,
			None => return PromoteResult::NotWarming,
		};
		if warming.overflowed {
			state.warming = None;
			return PromoteResult::Overflowed;
		}
		let buffered = state.warming.take().expect("warming present").buffered;
		let count = buffered.len();
		let batch_id = state.batch_id;
		let format = state.format;
		let sink = state.sink.clone();
		drop(state);
		if let Some(batch_id) = batch_id {
			let Some(batch) = self.batches.get(&batch_id) else {
				return PromoteResult::Disconnected;
			};
			{
				let now = self.clock.now_millis();
				let linger = batch.lingers.get(&subscription_id).copied().unwrap_or(Duration::ZERO);
				let mut entry = batch
					.pending
					.entry(subscription_id)
					.or_insert_with(|| MemberPending::new(linger));
				for columns in buffered {
					entry.push(Frame::from(columns), now);
				}
			}
			for waker in self.wakers.lock().iter() {
				waker.notify_one();
			}
		} else {
			for columns in buffered {
				match sink.send_change(subscription_id, columns, format) {
					DeliveryResult::Delivered => {}
					DeliveryResult::Disconnected => return PromoteResult::Disconnected,
				}
			}
		}
		PromoteResult::Promoted(count)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn register_batch(
		&self,
		connection_id: ConnectionId,
		members: Vec<(SubscriptionId, Duration)>,
		sink: S,
		format: S::Format,
		clock: &Clock,
		rng: &Rng,
	) -> BatchId {
		let batch_id = BatchId(Uuid7::generate(clock, rng));

		let member_ids: Vec<SubscriptionId> = members.iter().map(|(id, _)| *id).collect();
		let lingers: HashMap<SubscriptionId, Duration> = members.into_iter().collect();

		for member_id in &member_ids {
			if let Some(mut state) = self.subscriptions.get_mut(member_id) {
				state.batch_id = Some(batch_id);
			}
		}

		self.batches.insert(
			batch_id,
			BatchState {
				connection_id,
				sink,
				format,
				member_ids: member_ids.clone(),
				pending: DashMap::new(),
				lingers,
			},
		);

		self.connection_batches.entry(connection_id).or_default().push(batch_id);

		debug!(
			"Registered batch {} with {} members for connection {}",
			batch_id,
			member_ids.len(),
			connection_id
		);
		batch_id
	}

	pub fn unsubscribe_batch(&self, batch_id: BatchId) -> Option<Vec<SubscriptionId>> {
		let (_, state) = self.batches.remove(&batch_id)?;

		let connection_id = state.connection_id;
		let members = state.member_ids.clone();

		let mut removed_by_conn: HashMap<ConnectionId, HashSet<SubscriptionId>> = HashMap::new();
		for member_id in &members {
			if let Some((_, sub_state)) = self.subscriptions.remove(member_id) {
				removed_by_conn.entry(sub_state.connection_id).or_default().insert(*member_id);
			}
		}
		for (conn_id, removed) in removed_by_conn {
			if let Some(mut subs) = self.connections.get_mut(&conn_id) {
				subs.retain(|id| !removed.contains(id));
			}
		}

		let remove_connection =
			self.connections.get(&connection_id).map(|subs| subs.is_empty()).unwrap_or(false);
		if remove_connection {
			self.connections.remove(&connection_id);
		}

		let batches_empty = {
			if let Some(mut batches) = self.connection_batches.get_mut(&connection_id) {
				batches.retain(|id| *id != batch_id);
				batches.is_empty()
			} else {
				false
			}
		};
		if batches_empty {
			self.connection_batches.remove(&connection_id);
		}

		debug!("Unsubscribed batch {} ({} members)", batch_id, members.len());
		Some(members)
	}

	pub fn batch_for(&self, subscription_id: &SubscriptionId) -> Option<BatchId> {
		self.subscriptions.get(subscription_id).and_then(|state| state.batch_id)
	}

	pub fn batch_count(&self) -> usize {
		self.batches.len()
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
		{
			let now = self.clock.now_millis();
			let linger = batch.lingers.get(&subscription_id).copied().unwrap_or(Duration::ZERO);
			let mut entry =
				batch.pending.entry(subscription_id).or_insert_with(|| MemberPending::new(linger));
			for frame in frames {
				entry.push(frame, now);
			}
		}
		for waker in self.wakers.lock().iter() {
			waker.notify_one();
		}
		true
	}

	pub fn emit_batch_member_closed(&self, batch_id: BatchId, subscription_id: SubscriptionId) -> bool {
		let Some(batch) = self.batches.get(&batch_id) else {
			return false;
		};
		matches!(batch.sink.send_batch_member_closed(batch_id, subscription_id), DeliveryResult::Delivered)
	}

	pub fn unsubscribe(&self, subscription_id: SubscriptionId) -> bool {
		if let Some((_, state)) = self.subscriptions.remove(&subscription_id) {
			if !state.throttle.pending.is_empty() {
				self.throttle_pending.fetch_sub(1, Ordering::AcqRel);
			}
			let connection_id = state.connection_id;

			let should_remove_connection = {
				if let Some(mut subs) = self.connections.get_mut(&connection_id) {
					subs.retain(|id| *id != subscription_id);
					subs.is_empty()
				} else {
					false
				}
			};

			if should_remove_connection {
				self.connections.remove(&connection_id);
			}

			debug!("Unsubscribed subscription {}", subscription_id);
			true
		} else {
			false
		}
	}

	pub fn cleanup_connection(&self, connection_id: ConnectionId) -> Vec<SubscriptionId> {
		if let Some((_, batch_ids)) = self.connection_batches.remove(&connection_id) {
			for batch_id in &batch_ids {
				self.batches.remove(batch_id);
			}
		}

		if let Some((_, subscription_ids)) = self.connections.remove(&connection_id) {
			for sub_id in &subscription_ids {
				self.subscriptions.remove(sub_id);
			}
			debug!("Cleaned up subscriptions for disconnected connection {}", connection_id);
			subscription_ids
		} else {
			Vec::new()
		}
	}

	pub fn close_all(&self) {
		self.subscriptions.clear();
		self.batches.clear();
		self.connections.clear();
		self.connection_batches.clear();
		self.throttle_pending.store(0, Ordering::Release);
	}

	#[allow(dead_code)]
	pub fn subscription_count(&self) -> usize {
		self.subscriptions.len()
	}

	#[allow(dead_code)]
	pub fn connection_count(&self) -> usize {
		self.connections.len()
	}

	#[allow(dead_code)]
	pub fn log_stats(&self) {
		info!(
			"Registry stats: {} subscriptions, {} connections",
			self.subscriptions.len(),
			self.connections.len()
		);
	}
}

impl<S: WireSink> SubscriptionDelivery for SubscriptionRegistry<S> {
	fn try_deliver(&self, subscription_id: &SubscriptionId, columns: Columns) -> DeliveryResult {
		if let Some(batch_id) = self.batch_for(subscription_id) {
			if let Some(batch) = self.batches.get(&batch_id) {
				let now = self.clock.now_millis();
				let linger = batch.lingers.get(subscription_id).copied().unwrap_or(Duration::ZERO);
				batch.pending
					.entry(*subscription_id)
					.or_insert_with(|| MemberPending::new(linger))
					.push(Frame::from(columns), now);
				return DeliveryResult::Delivered;
			}

			return DeliveryResult::Disconnected;
		}

		let mut state = match self.subscriptions.get_mut(subscription_id) {
			Some(s) => s,
			None => return DeliveryResult::Disconnected,
		};

		if let Some(buffer) = state.warming.as_mut() {
			buffer.push(columns);
			return DeliveryResult::Delivered;
		}

		if state.throttle.enabled() {
			let now = self.clock.now_millis();
			if state.throttle.ready(now) && state.throttle.pending.is_empty() {
				let format = state.format;
				let sink = state.sink.clone();
				drop(state);
				match sink.send_change(*subscription_id, columns, format) {
					DeliveryResult::Delivered => {
						if let Some(mut s) = self.subscriptions.get_mut(subscription_id) {
							s.throttle.last_sent_at = Some(now);
						}
						DeliveryResult::Delivered
					}
					DeliveryResult::Disconnected => DeliveryResult::Disconnected,
				}
			} else {
				let was_empty = state.throttle.pending.is_empty();
				state.throttle.pending.push(columns);
				if was_empty {
					self.throttle_pending.fetch_add(1, Ordering::AcqRel);
				}
				DeliveryResult::Delivered
			}
		} else {
			let format = state.format;
			let sink = state.sink.clone();
			drop(state);
			sink.send_change(*subscription_id, columns, format)
		}
	}

	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.subscriptions.iter().map(|entry| *entry.key()).collect()
	}

	fn active_subscriptions_into(&self, out: &mut Vec<SubscriptionId>) {
		out.extend(self.subscriptions.iter().map(|entry| *entry.key()));
	}

	fn register_waker(&self, waker: Arc<Notify>) {
		self.wakers.lock().push(waker);
	}

	fn flush(&self) -> Option<Duration> {
		let now = self.clock.now_millis();
		let mut next_deadline: Option<u64> = None;

		if self.throttle_pending.load(Ordering::Acquire) > 0 {
			let mut throttle_ready: Vec<(SubscriptionId, Vec<Columns>, S::Format, S)> = Vec::new();

			for mut entry in self.subscriptions.iter_mut() {
				let sub_id = *entry.key();
				let state = entry.value_mut();
				if state.batch_id.is_some() || state.warming.is_some() {
					continue;
				}
				if !state.throttle.enabled() || state.throttle.pending.is_empty() {
					continue;
				}
				if !state.throttle.ready(now) {
					let rem = state.throttle.remaining_millis(now);
					next_deadline = Some(next_deadline.map_or(rem, |d| d.min(rem)));
					continue;
				}
				let drained = mem::take(&mut state.throttle.pending);
				self.throttle_pending.fetch_sub(1, Ordering::AcqRel);
				state.throttle.last_sent_at = Some(now);
				throttle_ready.push((sub_id, drained, state.format, state.sink.clone()));
			}

			let mut dead_subs: Vec<SubscriptionId> = Vec::new();
			for (sub_id, drained, format, sink) in throttle_ready {
				for columns in drained {
					match sink.send_change(sub_id, columns, format) {
						DeliveryResult::Delivered => {}
						DeliveryResult::Disconnected => {
							dead_subs.push(sub_id);
							break;
						}
					}
				}
			}
			for sub_id in dead_subs {
				self.unsubscribe(sub_id);
			}
		}

		if self.batches.is_empty() {
			return next_deadline.map(Duration::from_millis);
		}

		let mut dead_batches: Vec<BatchId> = Vec::new();

		for entry in self.batches.iter() {
			let batch_id = *entry.key();
			let batch = entry.value();

			let mut due: Vec<(SubscriptionId, Vec<Frame>)> = Vec::new();
			for mut e in batch.pending.iter_mut() {
				let key = *e.key();
				let member = e.value_mut();
				if member.frames.is_empty() {
					continue;
				}
				if member.ready(now) {
					let frames = mem::take(&mut member.frames);
					member.first_pending_at = None;
					due.push((key, frames));
				} else {
					let rem = member.remaining_millis(now);
					next_deadline = Some(next_deadline.map_or(rem, |d| d.min(rem)));
				}
			}
			if due.is_empty() {
				continue;
			}

			match batch.sink.send_batch_envelope(batch_id, batch.format, due) {
				DeliveryResult::Delivered => {}
				DeliveryResult::Disconnected => dead_batches.push(batch_id),
			}
		}

		for batch_id in dead_batches {
			if let Some(members) = self.unsubscribe_batch(batch_id) {
				debug!("Batch {} push channel closed; cascaded {} members", batch_id, members.len());
			}
		}

		next_deadline.map(Duration::from_millis)
	}
}
