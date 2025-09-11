// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreateSeries,
	plan::logical::{Compiler, LogicalPlan, resolver::IdentifierResolver},
};

impl Compiler {
	pub(crate) fn compile_create_series<
		'a,
		't,
		T: CatalogQueryTransaction,
	>(
		_ast: AstCreateSeries<'a>,
		_resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		unimplemented!()
	}
}
