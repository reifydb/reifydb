// Copyright (c) 2026 ReifyDB
// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::message::Envelope;

pub trait Transport: Send + Sync + 'static {
	fn send(&self, envelope: Envelope);

	fn receive(&self) -> Vec<Envelope>;
}
