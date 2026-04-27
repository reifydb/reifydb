// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_routine::routine::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::params::Params;

pub struct TransformContext<'a> {
	pub routines: &'a Routines,
	pub runtime_context: &'a RuntimeContext,
	pub params: &'a Params,
}
