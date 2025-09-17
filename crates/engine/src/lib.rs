// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod engine;
mod evaluate;
pub mod execute;
#[allow(dead_code)]
mod function;
pub(crate) mod interceptor;
pub mod table_virtual;
pub mod test_utils;
pub mod transaction;

pub use engine::StandardEngine;
pub use evaluate::StandardEvaluator;
use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
pub use reifydb_type::Result;
pub use transaction::{
	EngineTransaction, StandardCdcQueryTransaction, StandardCdcTransaction, StandardCommandTransaction,
	StandardQueryTransaction, StandardTransaction,
};

pub struct EngineVersion;

impl HasVersion for EngineVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "engine".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Query execution and processing engine module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
