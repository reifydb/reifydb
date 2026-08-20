// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_value::Result;

use crate::{
	actors::pending::Pending,
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, object::ObjectId},
		cdc::Cdc,
	},
};

pub type FlowActorHandle = ActorHandle<FlowActorMessage>;

pub enum FlowActorMessage {
	Drain,

	Wake,

	Loaded {
		outcome: Result<(Vec<Arc<Cdc>>, CommitVersion)>,
	},

	Tick,

	Sample,

	PublishRestoredFrontiers,

	UpdateSources {
		source_objects: Arc<BTreeSet<ObjectId>>,
		completeness_objects: Option<Arc<BTreeSet<u64>>>,
	},

	SliceCommitted {
		advance_to: CommitVersion,
		more: bool,
		result: Result<()>,
		committed: Option<(CommitVersion, Pending)>,
	},

	TickCommitted {
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
		flows: Vec<FlowId>,
		scan_from: Option<CommitVersion>,
	},

	Wake,

	PersistFrontiers,
}
