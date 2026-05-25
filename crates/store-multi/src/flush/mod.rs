// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Background flusher that migrates writes from the buffer tier to the persistent tier. The actor decides when to
//! flush (size, age, explicit request) and the listener exposes flush events to anything that wants to track
//! progress, like the admin UI or test harnesses waiting for durability.

pub mod actor;
pub mod listener;

use reifydb_core::interface::catalog::shape::ShapeId;

pub trait ShapePersistence: Send + Sync + 'static {
	fn is_persistent(&self, shape: ShapeId) -> bool;
}
