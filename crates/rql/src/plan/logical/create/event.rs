// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_rql_frontend::ast::ast::AstCreateEvent;
use reifydb_value::error::ValueError;

use crate::plan::logical::{Compiler, CreateEventNode, LogicalPlan};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_event(
		&self,
		ast: AstCreateEvent<'bump>,
	) -> Result<LogicalPlan<'bump>, ValueError> {
		Ok(LogicalPlan::CreateEvent(CreateEventNode {
			name: ast.name,
			variants: ast.variants,
		}))
	}
}
