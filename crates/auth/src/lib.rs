// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Turns a wire-level credential into a verified `IdentityId`. Authorisation belongs to the policy engine, so a
//! deployment can swap authentication methods without touching policy enforcement.
//!
//! Invariant: a successful authentication yields an `IdentityId` that resolves through the catalog to a real,
//! non-revoked identity. Minting one outside this crate bypasses revocation and is a security regression.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]
extern crate core;

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod challenge;
pub mod error;
pub mod github;
pub mod method;
pub mod registry;
pub mod service;

pub struct AuthVersion;

impl HasVersion for AuthVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Authentication and authorization module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
