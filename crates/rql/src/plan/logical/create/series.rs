// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::AstCreateSeries,
	plan::logical::{Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_series(&self, _ast: AstCreateSeries) -> crate::Result<LogicalPlan> {
		unimplemented!()
	}
}
