// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstCreateSeries,
	plan::logical::{Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_series(&self, _ast: AstCreateSeries) -> crate::Result<LogicalPlan> {
		unimplemented!()
	}
}
