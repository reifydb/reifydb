// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::params::Params;

pub struct TransformContext<'a> {
	pub routines: &'a Routines,
	pub runtime_context: &'a RuntimeContext,
	pub params: &'a Params,
}
