// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::VecDeque, sync::Arc};

use reifydb::{
	codec::frame::{encode::encode_frames, options::EncodeOptions},
	core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns},
	runtime::sync::mutex::Mutex,
	sub_core::{
		envelope::{BinaryKind, encode_rbcf_batch_envelope, encode_rbcf_envelope},
		wire_sink::{BatchSubscribedMember, ClientWireFormat, RawChangePayload, WireSink},
	},
	subscription::{batch::BatchId, delivery::DeliveryResult},
	value::value::{diff_type::DiffType, frame::frame::Frame},
};
use tracing::warn;

/// The only format this transport speaks.
///
/// The WebSocket transport also carries json and frames because a browser may ask for them over a
/// url. Nothing can ask this transport for anything: the caller is a node process that already
/// links the rbcf decoder, so a second encoding would be untested weight.
#[derive(Clone, Copy, Debug)]
pub struct Rbcf;

/// One thing the sink has to hand back to node.
///
/// Changes are already encoded because their byte layout is the contract clients decode against.
/// The close notifications are not: they carry two ids and no offsets, so there is nothing for a
/// second encoder to get subtly wrong, and node can surface them as plain values.
pub enum NodePush {
	Change {
		envelope: Vec<u8>,
	},
	BatchChange {
		envelope: Vec<u8>,
	},
	Closed {
		subscription_id: SubscriptionId,
	},
	BatchMemberClosed {
		batch_id: BatchId,
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

	fn send_subscribed(&self, _sub_id: SubscriptionId) -> DeliveryResult {
		// The subscription id goes back as the return value of the subscribe call, so there is
		// nothing to queue: node already has it before the first change can arrive.
		DeliveryResult::Delivered
	}

	fn send_batch_subscribed(&self, _batch_id: BatchId, _members: &[BatchSubscribedMember]) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_change(
		&self,
		sub_id: SubscriptionId,
		op: DiffType,
		columns: Columns,
		_format: Self::Format,
	) -> DeliveryResult {
		let frames = vec![Frame::from(columns).with_op(op)];
		match encode(&frames, sub_id) {
			Some(rbcf) => self.push(NodePush::Change {
				envelope: encode_rbcf_envelope(BinaryKind::Change, &sub_id.to_string(), &rbcf, None),
			}),
			None => DeliveryResult::Disconnected,
		}
	}

	fn send_remote_change(
		&self,
		sub_id: SubscriptionId,
		payload: RawChangePayload,
		_format: Self::Format,
	) -> DeliveryResult {
		// A remote change already arrived as rbcf unless the proxy had to fall back, so the
		// common case reframes bytes it never decodes.
		let rbcf = match payload {
			RawChangePayload::Rbcf(bytes) => bytes,
			other => match encode(&other.into_frames(), sub_id) {
				Some(bytes) => bytes,
				None => return DeliveryResult::Disconnected,
			},
		};
		self.push(NodePush::Change {
			envelope: encode_rbcf_envelope(BinaryKind::Change, &sub_id.to_string(), &rbcf, None),
		})
	}

	fn send_batch_envelope(
		&self,
		batch_id: BatchId,
		_format: Self::Format,
		entries: Vec<(SubscriptionId, Vec<Frame>)>,
	) -> DeliveryResult {
		let mut encoded: Vec<(String, Vec<u8>)> = Vec::with_capacity(entries.len());
		for (sub_id, frames) in entries {
			if let Some(rbcf) = encode(&frames, sub_id) {
				encoded.push((sub_id.to_string(), rbcf));
			}
		}
		if encoded.is_empty() {
			return DeliveryResult::Delivered;
		}
		self.push(NodePush::BatchChange {
			envelope: encode_rbcf_batch_envelope(&batch_id.to_string(), &encoded),
		})
	}

	fn send_batch_member_closed(&self, batch_id: BatchId, subscription_id: SubscriptionId) -> DeliveryResult {
		self.push(NodePush::BatchMemberClosed {
			batch_id,
			subscription_id,
		})
	}

	fn send_closed(&self, subscription_id: SubscriptionId) -> DeliveryResult {
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
