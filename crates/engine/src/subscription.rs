// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{result::Result as StdResult, sync::Arc};

use reifydb_core::{
	common::CommitVersion, interface::catalog::id::SubscriptionId, metric::ExecutionMetrics,
	value::column::columns::Columns,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_transaction::{multi::lease::VersionLeaseGuard, transaction::Transaction};
use reifydb_type::{Result, error::Error as TypeError, value::identity::IdentityId};

use crate::engine::StandardEngine;

#[derive(Debug)]
pub enum HydrateError {
	SubscriptionNotFound,
	UnsupportedSourceType,
	RowCapExceeded {
		cap: u64,
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
			Self::UnsupportedSourceType => "hydration is not supported for SourceFlow / SourceSeries / SourceInlineData; use WITH { hydration: { enabled: false } } to subscribe without it".to_string(),
			Self::RowCapExceeded { .. } => format!(
				"Hydration exceeds subscribe.max_hydration_rows={}; add `TAKE N` upstream, lower with WITH {{ hydration: {{ max_rows: ... }} }}, or disable with WITH {{ hydration: {{ enabled: false }} }}. Query: {}",
				cap, rql
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
	pub batches: Vec<Columns>,
	pub metrics: ExecutionMetrics,
}

pub trait SubscriptionService: Send + Sync {
	fn next_id(&self) -> SubscriptionId;

	fn register_subscription(
		&self,
		id: SubscriptionId,
		flow_dag: FlowDag,
		column_names: Vec<String>,
		txn: &mut Transaction<'_>,
	) -> Result<()>;

	fn unregister_subscription(&self, id: &SubscriptionId) -> Result<()>;

	fn hydrate(
		&self,
		sub_id: SubscriptionId,
		engine: &StandardEngine,
		identity: IdentityId,
		lease: VersionLeaseGuard,
		max_rows: u64,
	) -> StdResult<HydrateOutcome, HydrateError>;
}

pub type SubscriptionServiceRef = Arc<dyn SubscriptionService>;
