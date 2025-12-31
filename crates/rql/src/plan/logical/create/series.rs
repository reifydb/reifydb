// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreateSeries,
	plan::logical::{Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_series<'a, T: CatalogQueryTransaction>(
		_ast: AstCreateSeries,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		unimplemented!()
	}
}
