// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstCreateSeries,
	plan::logical::{Compiler, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_series(&self, _ast: AstCreateSeries<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		unimplemented!()
	}
}
