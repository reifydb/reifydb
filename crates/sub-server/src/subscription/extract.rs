// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_value::value::{Value, frame::frame::Frame};

pub fn extract_subscription_id(frames: &[Frame]) -> Option<SubscriptionId> {
	let frame = frames.first()?;
	frame.columns
		.iter()
		.find(|c| c.name == "subscription_id")
		.and_then(|col| {
			if !col.data.is_empty() {
				Some(col.data.get_value(0))
			} else {
				None
			}
		})
		.and_then(|value| match value {
			Value::Uint8(id) => Some(SubscriptionId(id)),
			_ => None,
		})
}
