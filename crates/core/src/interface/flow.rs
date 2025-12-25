// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Flow lag tracking interface for virtual table support.

use async_trait::async_trait;

use crate::interface::{PrimitiveId, catalog::FlowId};

/// A row in the system.flow_lags virtual table.
#[derive(Debug, Clone)]
pub struct FlowLagRow {
	/// The flow ID.
	pub flow_id: FlowId,
	/// The primitive this flow subscribes to.
	pub primitive_id: PrimitiveId,
	/// The lag: how many versions behind the flow is for this source.
	pub lag: u64,
}

/// Trait for providing flow lag data to virtual tables.
///
/// This trait is defined in the core crate to allow the engine crate
/// to use it without depending on the sub-flow crate.
///
/// Implemented by `FlowLagsProvider` in the sub-flow crate.
/// Used by the `FlowLags` virtual table in the engine crate.
#[async_trait]
pub trait FlowLagsProvider: Send + Sync {
	/// Get all flow lag rows.
	///
	/// Returns one row per (flow, source) pair, showing how far behind
	/// each flow is for each of its subscribed sources.
	async fn all_lags(&self) -> Vec<FlowLagRow>;
}
