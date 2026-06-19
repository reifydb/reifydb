// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_client::{RawChangePayload, WireFormat as ClientWireFormat};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_value::value::frame::frame::Frame;

pub struct BatchSubscribedMember {
	pub index: usize,
	pub subscription_id: SubscriptionId,
}

pub trait WireSink: Clone + Send + Sync + 'static {
	type Format: Copy + Send + Sync + Debug + 'static;

	fn client_wire_format(format: Self::Format) -> ClientWireFormat;

	fn send_subscribed(&self, sub_id: SubscriptionId) -> DeliveryResult;

	fn send_batch_subscribed(&self, batch_id: BatchId, members: &[BatchSubscribedMember]) -> DeliveryResult;

	fn send_change(&self, sub_id: SubscriptionId, columns: Columns, format: Self::Format) -> DeliveryResult;

	fn send_remote_change(
		&self,
		sub_id: SubscriptionId,
		payload: RawChangePayload,
		format: Self::Format,
	) -> DeliveryResult;

	fn send_batch_envelope(
		&self,
		batch_id: BatchId,
		format: Self::Format,
		entries: Vec<(SubscriptionId, Vec<Frame>)>,
	) -> DeliveryResult;

	fn send_batch_member_closed(&self, batch_id: BatchId, sub_id: SubscriptionId) -> DeliveryResult;

	fn send_closed(&self, sub_id: SubscriptionId) -> DeliveryResult;
}
