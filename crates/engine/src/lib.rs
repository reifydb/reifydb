// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_type::Result;

pub mod bulk_insert;
mod engine;
pub mod environment;
mod evaluate;
pub mod execute;
#[allow(dead_code)]
pub mod function;
pub(crate) mod interceptor;
pub mod stack;
pub mod stream;
pub mod table_virtual;
pub mod test_utils;
pub mod transaction;
pub mod util;

pub use engine::StandardEngine;
pub use evaluate::{
	ColumnEvaluationContext, RowEvaluationContext, column::StandardColumnEvaluator, row::StandardRowEvaluator,
};
pub use transaction::{StandardCommandTransaction, StandardQueryTransaction, StandardTransaction};

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
