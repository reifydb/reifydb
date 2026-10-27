// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::wire::WireFormat as ClientWireFormat;
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_subscription::delivery::DeliveryResult;
use reifydb_value::value::frame::frame::Frame;

#[derive(Debug, Clone)]
pub struct SubscribedEntry {
	pub index: usize,
	pub subscription_id: SubscriptionId,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChangeDelivery {
	Delivered {
		failed: Vec<SubscriptionId>,
	},
	Disconnected,
}

pub trait WireSink: Clone + Send + Sync + 'static {
	type Format: Copy + PartialEq + Send + Sync + Debug + 'static;

	fn client_wire_format(format: Self::Format) -> ClientWireFormat;

	fn send_subscribed(&self, subscriptions: &[SubscribedEntry]) -> DeliveryResult;

	fn send_change(&self, format: Self::Format, entries: Vec<(SubscriptionId, Vec<Frame>)>) -> ChangeDelivery;

	fn send_subscription_closed(&self, subscription_id: SubscriptionId) -> DeliveryResult;
}
