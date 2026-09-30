// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{result::Result as StdResult, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	flow::dag::FlowDag,
	interface::{catalog::id::SubscriptionId, change::StagedBatch},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_transaction::{multi::lease::VersionLeaseGuard, transaction::Transaction};
use reifydb_value::{
	Result,
	error::Error as TypeError,
	params::Params,
	value::{duration::Duration, identity::IdentityId, system_columns::SystemColumn},
};

use crate::engine::StandardEngine;

#[derive(Debug, Clone)]
pub struct SubscriptionContext {
	pub id: SubscriptionId,
	pub identity: IdentityId,
	pub symbols: SymbolTable,
	pub params: Params,
	pub named_system_columns: Vec<SystemColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HydrationBound {
	Present,
	Absent,
}

impl HydrationBound {
	pub fn advice(&self) -> String {
		match self {
			Self::Absent => "add `TAKE N` upstream, raise the hydration.max_rows subscribe option, or set the hydration.enabled subscribe option to false".to_string(),
			Self::Present => "the query's `TAKE` still returns more rows than the cap, so raise the hydration.max_rows subscribe option or set the hydration.enabled subscribe option to false".to_string(),
		}
	}
}

#[derive(Debug)]
pub enum HydrateError {
	SubscriptionNotFound,
	UnsupportedSourceType,
	RowCapExceeded {
		cap: u64,
		bound: HydrationBound,
	},
	Engine(TypeError),
	Internal(String),
}

impl From<TypeError> for HydrateError {
	fn from(e: TypeError) -> Self {
		HydrateError::Engine(e)
	}
}

impl HydrateError {
	pub fn is_version_evicted(&self) -> bool {
		matches!(self, HydrateError::Engine(e) if e.0.code == "TXN_012")
	}

	pub fn wire_code(&self) -> &'static str {
		match self {
			Self::SubscriptionNotFound => "HYDRATION_FAILED",
			Self::UnsupportedSourceType => "HYDRATION_UNSUPPORTED_SOURCE",
			Self::RowCapExceeded {
				..
			} => "HYDRATION_TOO_LARGE",
			Self::Engine(_) => {
				if self.is_version_evicted() {
					"HYDRATION_VERSION_EVICTED"
				} else {
					"HYDRATION_FAILED"
				}
			}
			Self::Internal(_) => "HYDRATION_FAILED",
		}
	}

	pub fn wire_message(&self, rql: &str, cap: u64) -> String {
		match self {
			Self::SubscriptionNotFound => "Subscription not found at hydration time".to_string(),
			Self::UnsupportedSourceType => "hydration is not supported for SourceSeries / SourceInlineData; set the hydration.enabled subscribe option to false to subscribe without it".to_string(),
			Self::RowCapExceeded {
				bound,
				..
			} => format!(
				"Hydration exceeds subscribe.max_hydration_rows={}; {}. Query: {}",
				cap,
				bound.advice(),
				rql
			),
			Self::Engine(e) => {
				if self.is_version_evicted() {
					e.0.message.clone()
				} else {
					e.to_string()
				}
			}
			Self::Internal(s) => s.clone(),
		}
	}
}

#[derive(Debug)]
pub struct HydrateOutcome {
	pub version: CommitVersion,
	pub batches: Vec<StagedBatch>,
	pub total: Duration,
}

pub trait SubscriptionService: Send + Sync {
	fn next_id(&self) -> SubscriptionId;

	fn register_subscription(
		&self,
		flow_dag: FlowDag,
		hydration_enabled: bool,
		ctx: SubscriptionContext,
		txn: &mut Transaction<'_>,
	) -> Result<()>;

	fn unregister_subscription(&self, id: &SubscriptionId) -> Result<bool>;

	fn hydrate(
		&self,
		sub_id: SubscriptionId,
		identity: IdentityId,
		lease: VersionLeaseGuard,
		max_rows: u64,
	) -> StdResult<HydrateOutcome, HydrateError>;
}

pub type SubscriptionServiceRef = Arc<dyn SubscriptionService>;

#[cfg(feature = "testing")]
pub trait HandOffHooks: Send + Sync {
	fn during_hand_off(&self, _subscription: SubscriptionId) {}
}

#[cfg(feature = "testing")]
#[derive(Clone)]
pub struct InstalledHandOffHooks(pub Arc<dyn HandOffHooks>);

pub fn acquire_hand_off_lease(
	engine: &StandardEngine,
	_subscriptions: &[SubscriptionId],
) -> Result<(CommitVersion, VersionLeaseGuard)> {
	#[cfg(feature = "testing")]
	if let Some(InstalledHandOffHooks(hooks)) = engine.ioc().try_resolve::<InstalledHandOffHooks>() {
		for subscription in _subscriptions {
			hooks.during_hand_off(*subscription);
		}
	}
	engine.acquire_current_snapshot_lease()
}
