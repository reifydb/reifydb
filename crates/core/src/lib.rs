// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Foundational types, traits, on-disk encodings, and runtime primitives shared across the entire ReifyDB workspace.
//!
//! Every other crate in the workspace depends on `core`, and `core` depends on no other ReifyDB crate (apart from
//! `reifydb-type` and `reifydb-runtime`). Its purpose is to break what would otherwise be a forest of circular
//! dependencies between the storage tier, query engine, catalog, transaction manager, policy enforcer, and
//! subscription/flow runtime: each of those crates implements traits defined here and consumes data shapes defined
//! here. Nothing in this crate knows about a specific storage backend, query engine, or catalog implementation - it is
//! the shared vocabulary that lets the rest of the system talk to itself.
//!
//! The cross-crate contract surface defines the catalog object hierarchy, the storage backend trait, the
//! change-data-capture contract, the expression-evaluation contract, the dataflow-graph contract, the authentication
//! contract, and component version reporting. When a crate downstream of `core` needs to expose itself to another
//! crate, it does so by implementing one of these traits.
//!
//! The on-disk format lives here. Every key kind is enumerated in `KeyKind` (a `repr(u8)` with one byte per catalog
//! object kind and per system structure) with a typed key per kind that encodes and decodes itself to the canonical
//! byte layout. The binary representation of every primitive type - integers, floats, booleans, blobs, temporals,
//! decimals, plus identity and dictionary references - lives alongside it. Storage backends, replication, and the wire
//! protocol all serialise through these encodings.
//!
//! The runtime data model lives here. The in-memory representation of query results - columns, frames, batches, and
//! the row-oriented views over them - is what the engine produces and consumers (subscriptions, the wire layer, the
//! SDK) read. Change records emitted by writes, row-shape primitives, the typed event bus that crates use to publish
//! and subscribe across actor boundaries, and the canonical catalogue of long-lived background actors all live in this
//! tier; declaring an actor here is what makes it discoverable to the runtime supervisor.
//!
//! The remaining modules are infrastructural. `CoreError` and the diagnostic machinery render user-visible failures
//! with source-fragment context. Stable content hashes support plan caching and change detection. Shared primitives -
//! execution, retention, sort, metric - are consumed by the engine and the actor system. A grab-bag of dependency-free
//! helpers (bloom filters, LRU caches, an inversion-of-control container, retry policies, slab allocators) is
//! intentionally kept independent so it can be used anywhere. Capture types for the test harness are exposed
//! separately.
//!
//! Invariant: `core` does not depend on any other ReifyDB crate beyond `reifydb-type` and `reifydb-runtime`. Adding
//! such a dependency would re-introduce the cycles this crate exists to break; new shared functionality belongs here,
//! not in a downstream crate that `core` would then have to import back.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use crate::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod actors;
pub mod common;
pub mod delta;
pub mod encoded;
pub mod error;
pub mod event;
pub mod execution;
pub mod fingerprint;
pub mod interface;
pub mod key;
pub mod metric;
pub mod retention;
pub mod row;
pub mod sort;
pub mod testing;
pub mod util;
pub mod value;

pub struct CoreVersion;

impl HasVersion for CoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Core database interfaces and data structures".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
