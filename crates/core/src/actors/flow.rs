// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorHandle},
	sync::rwlock::RwLock,
};
use reifydb_value::error::ValueError;

use crate::{
	common::CommitVersion,
	delta::RemoveVisibility,
	interface::{
		catalog::{flow::FlowId, id::ViewId, object::ObjectId},
		commit::Loaded,
	},
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

	PublishRestoredOutputWatermarks,

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

	RemoveExpired {
		keys: Vec<EncodedKey>,
		visibility: RemoveVisibility,
	},

	ExpireCommitted {
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

#[derive(Clone, Default)]
pub struct FlowWriterRegistry {
	writers: Arc<RwLock<BTreeMap<ViewId, (FlowId, ActorRef<FlowActorMessage>)>>>,
}

impl FlowWriterRegistry {
	pub fn new() -> Self {
		Self {
			writers: Arc::new(RwLock::new(BTreeMap::new())),
		}
	}

	pub fn register(&self, view: ViewId, head: FlowId, actor: ActorRef<FlowActorMessage>) {
		self.writers.write().insert(view, (head, actor));
	}

	pub fn unregister_flow(&self, head: FlowId) {
		self.writers.write().retain(|_, (flow, _)| *flow != head);
	}

	pub fn find(&self, view: ViewId) -> Option<ActorRef<FlowActorMessage>> {
		self.writers.read().get(&view).map(|(_, actor)| actor.clone())
	}
}
