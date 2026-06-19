// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	result::Result as StdResult,
	sync::{Arc, mpsc},
};

use reifydb_core::{
	error::diagnostic::catalog::subscription_not_found,
	interface::catalog::{flow::FlowId, id::SubscriptionId},
	internal,
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, HydrateOutcome, SubscriptionService},
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::{actor::mailbox::ActorRef, sync::rwlock::RwLock};
use reifydb_transaction::{
	multi::{lease::VersionLeaseGuard, transaction::MultiTransaction},
	transaction::Transaction,
};
use reifydb_value::{Result, error::Error, fragment::Fragment, value::identity::IdentityId};

use crate::{store::SubscriptionStore, tracker::SubscriptionPositionTracker, worker::SubscriptionWorkerMessage};

pub(super) struct SubscriptionState {
	pub(super) store: Arc<SubscriptionStore>,
	pub(super) workers: Vec<ActorRef<SubscriptionWorkerMessage>>,
	pub(super) subscription_flows: RwLock<HashMap<SubscriptionId, FlowId>>,
	pub(super) multi: MultiTransaction,
	pub(super) position_tracker: SubscriptionPositionTracker,
}

impl SubscriptionState {
	fn worker_for(&self, flow_id: FlowId) -> &ActorRef<SubscriptionWorkerMessage> {
		let index = (flow_id.0 as usize) % self.workers.len();
		&self.workers[index]
	}
}

pub(super) struct SubscriptionServiceImpl {
	pub(super) state: Arc<SubscriptionState>,
}

impl SubscriptionServiceImpl {
	fn resolve_flow_id(&self, sub_id: SubscriptionId) -> StdResult<FlowId, HydrateError> {
		self.state.subscription_flows.read().get(&sub_id).copied().ok_or(HydrateError::SubscriptionNotFound)
	}
}

impl SubscriptionService for SubscriptionServiceImpl {
	fn next_id(&self) -> SubscriptionId {
		self.state.store.next_id()
	}

	fn register_subscription(
		&self,
		id: SubscriptionId,
		flow_dag: FlowDag,
		column_names: Vec<String>,
		hydration_enabled: bool,
		_txn: &mut Transaction<'_>,
	) -> Result<()> {
		self.state.store.register(id, column_names);

		let current = self.state.multi.begin_query()?.version();
		self.state.position_tracker.update(id, current);

		let flow_id = flow_dag.id;
		let gate = if hydration_enabled {
			Some(current)
		} else {
			None
		};

		let (tx, rx) = mpsc::channel();
		let reply: Box<dyn FnOnce(Result<()>) + Send> = Box::new(move |r| {
			let _ = tx.send(r);
		});
		self.state
			.worker_for(flow_id)
			.send(SubscriptionWorkerMessage::Register {
				id,
				flow_id,
				flow_dag,
				gate,
				reply,
			})
			.map_err(|_| Error(Box::new(internal!("subscription worker unavailable"))))?;
		let registered =
			rx.recv().map_err(|_| Error(Box::new(internal!("subscription worker dropped reply"))))?;
		registered?;

		self.state.subscription_flows.write().insert(id, flow_id);
		Ok(())
	}

	fn unregister_subscription(&self, id: &SubscriptionId) -> Result<()> {
		let existed = self.state.store.unregister(id);
		self.state.position_tracker.remove(id);

		if let Some(flow_id) = self.state.subscription_flows.write().remove(id) {
			let (tx, rx) = mpsc::channel();
			let reply: Box<dyn FnOnce() + Send> = Box::new(move || {
				let _ = tx.send(());
			});
			if self.state
				.worker_for(flow_id)
				.send(SubscriptionWorkerMessage::Unregister {
					flow_id,
					reply,
				})
				.is_ok()
			{
				let _ = rx.recv();
			}
		}

		if existed {
			Ok(())
		} else {
			Err(Error(Box::new(subscription_not_found(
				Fragment::internal(format!("subscription_{}", id.0)),
				&format!("subscription_{}", id.0),
			))))
		}
	}

	fn hydrate(
		&self,
		sub_id: SubscriptionId,
		_engine: &StandardEngine,
		identity: IdentityId,
		lease: VersionLeaseGuard,
		max_rows: u64,
	) -> StdResult<HydrateOutcome, HydrateError> {
		let flow_id = self.resolve_flow_id(sub_id)?;

		let (tx, rx) = mpsc::channel();
		let reply: Box<dyn FnOnce(StdResult<HydrateOutcome, HydrateError>) + Send> = Box::new(move |r| {
			let _ = tx.send(r);
		});
		self.state
			.worker_for(flow_id)
			.send(SubscriptionWorkerMessage::Hydrate {
				sub_id,
				flow_id,
				identity,
				lease,
				max_rows,
				reply,
			})
			.map_err(|_| HydrateError::Internal("subscription worker unavailable".to_string()))?;
		rx.recv()
			.map_err(|_| HydrateError::Internal("subscription worker dropped hydrate reply".to_string()))?
	}
}
