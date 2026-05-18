// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Authentication: turning a wire-level credential into a verified `IdentityId` the rest of the system can attach
//! to a transaction. The crate owns the registry of supported authentication methods, the challenge-response state
//! machine for methods that need it, and the service handle the server tiers route incoming sessions through.
//!
//! Authorisation - what an identity is allowed to do once authenticated - is not in this crate; that is the policy
//! engine's responsibility. The split exists so a deployment can swap out authentication methods (token, password,
//! external IDP) without touching the policy enforcement path.
//!
//! Invariant: a successful authentication produces an `IdentityId` that resolves through the catalog to a real,
//! non-revoked identity. Anything that mints an `IdentityId` outside this crate (test fixtures aside) bypasses
//! revocation and method requirements and is a security regression.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]
extern crate core;

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod challenge;
pub mod error;
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
