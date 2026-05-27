// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Background flusher that migrates writes from the buffer tier to the persistent tier. Flush is the
//! watermark-coupled eviction sweep: on each tick (or explicit request) it persists the latest-<=W value per key of
//! every persistent shape, then drops all <=W versions from the commit tier, bounding the commit tier's RAM.

pub mod actor;

use reifydb_core::interface::catalog::shape::ShapeId;

pub trait ShapePersistence: Send + Sync + 'static {
	fn is_persistent(&self, shape: ShapeId) -> bool;
}
