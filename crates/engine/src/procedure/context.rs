// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::auth::Identity;
use reifydb_function::registry::Functions;
use reifydb_runtime::clock::Clock;
use reifydb_type::params::Params;

pub struct ProcedureContext<'a> {
	pub identity: &'a Identity,
	pub params: &'a Params,
	pub catalog: &'a Catalog,
	pub functions: &'a Functions,
	pub clock: &'a Clock,
}
