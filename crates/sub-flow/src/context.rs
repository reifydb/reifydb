// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::TimeDomain;
use reifydb_engine::vm::stack::SymbolTable;
use reifydb_value::{params::Params, value::identity::IdentityId};

#[derive(Debug, Clone)]
pub struct FlowContext {
	pub identity: IdentityId,
	pub symbols: SymbolTable,
	pub params: Params,
	pub time: TimeDomain,
}

impl Default for FlowContext {
	fn default() -> Self {
		Self {
			identity: IdentityId::root(),
			symbols: SymbolTable::new(),
			params: Params::None,
			time: TimeDomain::Processing,
		}
	}
}
