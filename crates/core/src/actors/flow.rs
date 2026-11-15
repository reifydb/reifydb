// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_value::error::ValueError;

use crate::{
	common::CommitVersion,
	interface::{catalog::object::ObjectId, commit::Loaded},
};

pub type FlowActorHandle = ActorHandle<FlowActorMessage>;

pub enum FlowActorMessage {
	Drain,

	Wake,

	TrailingWake,

	Loaded {
		outcome: Result<Loaded, ValueError>,
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
		result: Result<(), ValueError>,
		committed: Option<CommitVersion>,
	},

	TickCommitted {
		result: Result<(), ValueError>,
	},

	CheckpointNow {
		reply: Box<dyn FnOnce() + Send>,
	},

	Stop {
		delete_checkpoint: bool,
		reply: Box<dyn FnOnce() + Send>,
	},
}
