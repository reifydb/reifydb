// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_routine::function::registry::Functions;
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::params::Params;

pub struct TransformContext<'a> {
	pub functions: &'a Functions,
	pub runtime_context: &'a RuntimeContext,
	pub params: &'a Params,
}
