// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::params::Params;

use crate::function::registry::Functions;

pub struct ProcedureContext<'a> {
	pub params: &'a Params,
	pub catalog: &'a Catalog,
	pub functions: &'a Functions,
	pub runtime_context: &'a RuntimeContext,
}
