// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use reifydb_type::value::frame::frame::Frame;
use reifydb_wire_format::{encode::encode_frames, options::EncodeOptions};
use tokio::sync::mpsc;
use tonic::Status;
use tracing::debug;

use crate::{
	convert::frames_to_proto,
	generated::{ChangeEvent, Format, FramesPayload, SubscriptionEvent, change_event, subscription_event},
};

/// Wire format chosen by the client for a given subscription.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WireFormat {
	#[default]
	Proto,
	Rbcf,
}

impl WireFormat {
	/// Resolve from the on-wire i32 value of the proto `Format` enum.
	/// `FORMAT_UNSPECIFIED` is treated as `FORMAT_PROTO` for backwards compatibility.
	pub fn from_proto_i32(format: i32) -> Self {
		match Format::try_from(format).unwrap_or(Format::Unspecified) {
			Format::Rbcf => WireFormat::Rbcf,
			Format::Proto | Format::Unspecified => WireFormat::Proto,
		}
	}
}

struct SubscriptionState {
	tx: mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>,
	format: WireFormat,
}

pub struct GrpcSubscriptionRegistry {
	subscriptions: DashMap<SubscriptionId, SubscriptionState>,
}

impl Default for GrpcSubscriptionRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl GrpcSubscriptionRegistry {
	pub fn new() -> Self {
		Self {
			subscriptions: DashMap::new(),
		}
	}

	pub fn register(
		&self,
		subscription_id: SubscriptionId,
		tx: mpsc::UnboundedSender<Result<SubscriptionEvent, Status>>,
		format: WireFormat,
	) {
		self.subscriptions.insert(
			subscription_id,
			SubscriptionState {
				tx,
				format,
			},
		);
		debug!("Registered gRPC subscription {} (format={:?})", subscription_id, format);
	}

	pub fn unregister(&self, subscription_id: &SubscriptionId) {
		self.subscriptions.remove(subscription_id);
		debug!("Unregistered gRPC subscription {}", subscription_id);
	}

	pub fn close_all(&self) {
		self.subscriptions.clear();
	}
}

impl SubscriptionDelivery for GrpcSubscriptionRegistry {
	fn try_deliver(&self, subscription_id: &SubscriptionId, columns: Columns) -> DeliveryResult {
		let (tx, format) = match self.subscriptions.get(subscription_id) {
			Some(entry) => {
				let state = entry.value();
				(state.tx.clone(), state.format)
			}
			None => return DeliveryResult::Disconnected,
		};

		let frames = vec![Frame::from(columns)];
		let payload = match format {
			WireFormat::Rbcf => {
				let rbcf = encode_frames(&frames, &EncodeOptions::fast()).unwrap_or_default();
				change_event::Payload::Rbcf(rbcf)
			}
			WireFormat::Proto => change_event::Payload::Frames(FramesPayload {
				frames: frames_to_proto(frames),
			}),
		};

		let event = SubscriptionEvent {
			event: Some(subscription_event::Event::Change(ChangeEvent {
				payload: Some(payload),
			})),
		};

		match tx.send(Ok(event)) {
			Ok(_) => DeliveryResult::Delivered,
			Err(_) => DeliveryResult::Disconnected,
		}
	}

	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.subscriptions.iter().map(|entry| *entry.key()).collect()
	}
}
