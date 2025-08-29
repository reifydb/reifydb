// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstDistinct,
	plan::logical::{Compiler, DistinctNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_distinct<'a>(
		ast: AstDistinct<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		Ok(LogicalPlan::Distinct(DistinctNode {
			columns: ast
				.columns
				.into_iter()
				.map(|col| col.fragment())
				.collect(),
		}))
	}
}
