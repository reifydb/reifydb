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
	plan::logical::{Compiler, LogicalPlan, resolver::IdentifierResolver},
};

impl Compiler {
	pub(crate) fn compile_create<'a, 't, T: CatalogQueryTransaction>(
		ast: AstCreate<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		match ast {
			AstCreate::DeferredView(node) => {
				Self::compile_deferred_view(node, resolver)
			}
			AstCreate::TransactionalView(node) => {
				Self::compile_transactional_view(node, resolver)
			}
			AstCreate::Namespace(node) => {
				Self::compile_create_namespace(node, resolver)
			}
			AstCreate::Series(node) => {
				Self::compile_create_series(node, resolver)
			}
			AstCreate::Table(node) => {
				Self::compile_create_table(node, resolver)
			}
			AstCreate::RingBuffer(node) => {
				Self::compile_create_ring_buffer(node, resolver)
			}
			AstCreate::Index(node) => {
				Self::compile_create_index(node, resolver)
			}
		}
	}
}
