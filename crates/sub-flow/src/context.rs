// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::vm::stack::SymbolTable;
use reifydb_value::{params::Params, value::identity::IdentityId};

#[derive(Debug, Clone)]
pub struct FlowContext {
	pub identity: IdentityId,
	pub symbols: SymbolTable,
	pub params: Params,
}

impl Default for FlowContext {
	fn default() -> Self {
		Self {
			identity: IdentityId::root(),
			symbols: SymbolTable::new(),
			params: Params::None,
		}
	}
}
