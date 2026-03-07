// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use dashmap::DashMap;
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use reifydb_type::value::frame::frame::Frame;
use tokio::sync::mpsc;
use tonic::Status;
use tracing::{debug, warn};

use crate::{
	convert::frames_to_proto,
	generated::{ChangeEvent, SubscriptionEvent, subscription_event},
};

pub struct GrpcSubscriptionRegistry {
	subscriptions: DashMap<SubscriptionId, mpsc::Sender<Result<SubscriptionEvent, Status>>>,
}

impl GrpcSubscriptionRegistry {
	pub fn new() -> Self {
		Self {
			subscriptions: DashMap::new(),
		}
	}

	pub fn register(&self, subscription_id: SubscriptionId, tx: mpsc::Sender<Result<SubscriptionEvent, Status>>) {
		self.subscriptions.insert(subscription_id, tx);
		debug!("Registered gRPC subscription {}", subscription_id);
	}

	pub fn unregister(&self, subscription_id: &SubscriptionId) {
		self.subscriptions.remove(subscription_id);
		debug!("Unregistered gRPC subscription {}", subscription_id);
	}
}

impl SubscriptionDelivery for GrpcSubscriptionRegistry {
	fn try_deliver(&self, subscription_id: &SubscriptionId, columns: Columns) -> DeliveryResult {
		let tx = match self.subscriptions.get(subscription_id) {
			Some(entry) => entry.value().clone(),
			None => return DeliveryResult::Disconnected,
		};

		let frames = vec![Frame::from(columns)];
		let proto_frames = frames_to_proto(frames);

		let event = SubscriptionEvent {
			event: Some(subscription_event::Event::Change(ChangeEvent {
				frames: proto_frames,
			})),
		};

		match tx.try_send(Ok(event)) {
			Ok(_) => DeliveryResult::Delivered,
			Err(mpsc::error::TrySendError::Full(_)) => {
				warn!("Back pressure for gRPC subscription {}", subscription_id);
				DeliveryResult::BackPressure
			}
			Err(mpsc::error::TrySendError::Closed(_)) => DeliveryResult::Disconnected,
		}
	}

	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.subscriptions.iter().map(|entry| *entry.key()).collect()
	}
}
