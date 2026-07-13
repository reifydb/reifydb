// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_value::Result;

use crate::{
	actors::pending::Pending,
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		cdc::Cdc,
	},
};

pub type FlowActorHandle = ActorHandle<FlowActorMessage>;

pub enum FlowActorMessage {
	Drain,

	Wake,

	Ingest {
		cdcs: Arc<Vec<Cdc>>,
		covers_from: CommitVersion,
		up_to: CommitVersion,
	},

	Tick,

	UpdateSources {
		source_shapes: Arc<BTreeSet<ShapeId>>,
	},

	CommitDone {
		advance_to: CommitVersion,
		more: bool,
		result: Result<()>,
		committed: Option<(CommitVersion, Pending)>,
	},

	Stop {
		delete_checkpoint: bool,
		reply: Box<dyn FnOnce() + Send>,
	},
}

pub type FlowSupervisorHandle = ActorHandle<FlowSupervisorMessage>;

pub enum FlowSupervisorMessage {
	Bootstrap {
		flows: Vec<(FlowId, bool)>,
	},

	Consume {
		cdcs: Vec<Cdc>,
		current_version: CommitVersion,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	},
}
