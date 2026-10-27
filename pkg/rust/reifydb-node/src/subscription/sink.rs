// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::VecDeque, sync::Arc};

use reifydb::{
	codec::{
		frame::{encode::encode_frames, options::EncodeOptions},
		wire::WireFormat as ClientWireFormat,
	},
	core::interface::catalog::id::SubscriptionId,
	runtime::sync::mutex::Mutex,
	sub_core::{
		envelope::encode_rbcf_change_envelope,
		wire_sink::{ChangeDelivery, SubscribedEntry, WireSink},
	},
	subscription::delivery::DeliveryResult,
	value::value::frame::frame::Frame,
};
use tracing::warn;

/// The only format this transport speaks.
///
/// The WebSocket transport also carries json and frames because a browser may ask for them over a
/// url. Nothing can ask this transport for anything: the caller is a node process that already
/// links the rbcf decoder, so a second encoding would be untested weight.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rbcf;

/// One thing the sink has to hand back to node.
///
/// Changes are already encoded because their byte layout is the contract clients decode against.
/// A close notification is not: it carries one id and no offsets, so there is nothing for a
/// second encoder to get subtly wrong, and node can surface it as a plain value.
pub enum NodePush {
	Change {
		envelope: Vec<u8>,
	},
	Closed {
		subscription_id: SubscriptionId,
	},
}

/// Holds what the subscription machinery produced until node asks for it.
///
/// The WebSocket sink owns the sending half of a channel and a task drains it. There is no task
/// here on purpose: under dst the calling thread is the only one that runs anything, so delivery
/// has to be something the caller pulls rather than something a loop pushes.
#[derive(Clone)]
pub struct NodeWireSink {
	queue: Arc<Mutex<VecDeque<NodePush>>>,
}

impl NodeWireSink {
	pub fn new() -> Self {
		Self {
			queue: Arc::new(Mutex::new(VecDeque::new())),
		}
	}

	/// Takes everything queued so far, leaving the sink empty.
	pub fn drain(&self) -> Vec<NodePush> {
		let mut queue = self.queue.lock();
		queue.drain(..).collect()
	}

	pub fn pending(&self) -> usize {
		self.queue.lock().len()
	}

	#[inline]
	fn push(&self, message: NodePush) -> DeliveryResult {
		self.queue.lock().push_back(message);
		DeliveryResult::Delivered
	}
}

impl Default for NodeWireSink {
	fn default() -> Self {
		Self::new()
	}
}

impl WireSink for NodeWireSink {
	type Format = Rbcf;

	fn client_wire_format(_format: Self::Format) -> ClientWireFormat {
		ClientWireFormat::Rbcf
	}

	fn send_subscribed(&self, _subscriptions: &[SubscribedEntry]) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_change(&self, _format: Self::Format, entries: Vec<(SubscriptionId, Vec<Frame>)>) -> ChangeDelivery {
		let mut encoded: Vec<(String, Vec<u8>)> = Vec::with_capacity(entries.len());
		let mut failed = Vec::new();
		for (sub_id, frames) in entries {
			match encode(&frames, sub_id) {
				Some(rbcf) => encoded.push((sub_id.to_string(), rbcf)),
				None => failed.push(sub_id),
			}
		}
		if !encoded.is_empty() {
			self.push(NodePush::Change {
				envelope: encode_rbcf_change_envelope(&encoded),
			});
		}
		ChangeDelivery::Delivered {
			failed,
		}
	}

	fn send_subscription_closed(&self, subscription_id: SubscriptionId) -> DeliveryResult {
		self.push(NodePush::Closed {
			subscription_id,
		})
	}
}

#[inline]
fn encode(frames: &[Frame], sub_id: SubscriptionId) -> Option<Vec<u8>> {
	match encode_frames(frames, &EncodeOptions::fast()) {
		Ok(bytes) => Some(bytes),
		Err(e) => {
			warn!("Failed to RBCF-encode change for {}: {}", sub_id, e);
			None
		}
	}
}
