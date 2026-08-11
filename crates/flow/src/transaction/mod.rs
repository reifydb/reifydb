// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{actors::pending::PendingLayers, common::CommitVersion};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{interceptor::interceptors::Interceptors, multi::transaction::read::MultiReadTransaction};
use reifydb_value::value::datetime::DateTime;

pub mod deferred;
pub mod dictionary;
pub mod ephemeral;
pub mod frontier;
pub mod group;
pub mod interface;
pub mod read;
pub mod reclaim;
pub mod row_number;
pub mod slot;
pub mod state;
pub mod substrate;
pub mod timer;
pub mod watermark;
pub mod write;

use crate::transaction::substrate::FlowSubstrate;

#[derive(Clone, Copy)]
pub struct ChangeCoordinate {
	pub at: Option<DateTime>,
	pub version: CommitVersion,
}

pub struct DeferredParams {
	pub version: CommitVersion,
	pub pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub substrate: FlowSubstrate,
}
