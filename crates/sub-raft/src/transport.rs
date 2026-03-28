// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use crate::message::Envelope;

/// Abstraction for delivering Raft messages between nodes.
///
/// Implementations range from in-process channels (for testing)
/// to gRPC (production). All implementations must be thread-safe.
/// Messages may be dropped or reordered — Raft tolerates this.
pub trait Transport: Send + Sync + 'static {
	/// Enqueue an outbound message for delivery. Non-blocking.
	fn send(&self, envelope: Envelope);

	/// Drain all inbound messages received since the last call.
	fn receive(&self) -> Vec<Envelope>;
}
