// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use crate::message::Envelope;

pub trait Transport: Send + Sync + 'static {
	fn send(&self, envelope: Envelope);

	fn receive(&self) -> Vec<Envelope>;
}
