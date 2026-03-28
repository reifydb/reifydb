// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker

use std::any::Any;

use crate::log::{Entry, Index};

pub mod apply;
pub mod testing;

/// A Raft-managed state machine. Commands are applied sequentially from the
/// Raft log and must be deterministic across all nodes.
pub trait State: Send {
	/// Returns the last applied log index.
	fn get_applied_index(&self) -> Index;

	/// Applies a log entry to the state machine.
	fn apply(&mut self, entry: &Entry);

	/// Returns self as `Any` for downcasting (e.g. to `KV` in tests).
	fn as_any(&self) -> &dyn Any;
}
