// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod deferred;
mod index;
mod namespace;
mod ring_buffer;
mod series;
mod table;
mod transactional;

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreate,
	plan::logical::{Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create<'a, T: CatalogQueryTransaction>(
		ast: AstCreate<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		match ast {
			AstCreate::DeferredView(node) => Self::compile_deferred_view(node, tx),
			AstCreate::TransactionalView(node) => Self::compile_transactional_view(node, tx),
			AstCreate::Flow(_) => {
				// TODO: Implement CREATE FLOW compilation
				todo!("CREATE FLOW compilation not yet implemented")
			}
			AstCreate::Namespace(node) => Self::compile_create_namespace(node, tx),
			AstCreate::Series(node) => Self::compile_create_series(node, tx),
			AstCreate::Table(node) => Self::compile_create_table(node, tx),
			AstCreate::RingBuffer(node) => Self::compile_create_ring_buffer(node, tx),
			AstCreate::Index(node) => Self::compile_create_index(node, tx),
		}
	}
}
