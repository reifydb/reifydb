// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Change Data Capture: durable record of every committed write, ordered, addressable, and consumable by external
//! systems. The transaction layer hands committed deltas here at commit time; this crate persists them, exposes the
//! consumer-side cursor APIs that downstream replication, subscriptions, and external sinks read from, and runs the
//! background compactor that prunes records past retention.
//!
//! Producers and consumers are decoupled - a write only needs to be persisted before the transaction returns; the
//! consumer side can fall arbitrarily far behind and catch up later. Storage is pluggable so the same protocol can
//! be backed by SQLite for embedded deployments and by a horizontally scaled log for production.
//!
//! Invariant: a CDC record published for a transaction reflects exactly the deltas that were committed under that
//! transaction id, in the order the engine produced them. Reordering or dropping deltas inside CDC desynchronises
//! replicas and subscriptions from the source of truth.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod compact;
pub mod consume;
pub mod error;
pub mod produce;
pub mod storage;
pub mod testing;

pub struct CdcVersion;

impl HasVersion for CdcVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Change Data Capture module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
