// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_function::registry::Functions;
use reifydb_runtime::clock::Clock;
use reifydb_type::params::Params;

pub struct TransformContext<'a> {
	pub functions: &'a Functions,
	pub clock: &'a Clock,
	pub params: &'a Params,
}
