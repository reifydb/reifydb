// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod clock;
pub mod set;

use reifydb_catalog::procedure::registry::{Procedures, ProceduresBuilder};

pub fn default_procedures() -> ProceduresBuilder {
	Procedures::builder()
		.with_procedure("system::config::set", set::config::SetConfigProcedure::new)
		.with_procedure("clock::set", clock::set::ClockSetProcedure::new)
		.with_procedure("clock::advance", clock::advance::ClockAdvanceProcedure::new)
}
