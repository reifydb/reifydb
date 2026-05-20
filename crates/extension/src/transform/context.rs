// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_routine::routine::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::params::Params;

pub struct TransformContext<'a> {
	pub routines: &'a Routines,
	pub runtime_context: &'a RuntimeContext,
	pub params: &'a Params,
}
