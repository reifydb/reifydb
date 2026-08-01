// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Change Data Capture: a durable, ordered, addressable record of every committed write. Producers and
//! consumers are decoupled, so a consumer may fall arbitrarily far behind and catch up later.
//!
//! Invariant: a CDC record reflects exactly the deltas committed under its transaction id, in the order the
//! engine produced them. Reordering or dropping deltas here desynchronises replicas and subscriptions.

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
