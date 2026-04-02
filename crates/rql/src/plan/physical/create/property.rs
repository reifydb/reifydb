// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result,
	ast::identifier::MaybeQualifiedColumnShape,
	nodes::CreateColumnPropertyNode,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_column_property(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateColumnPropertyNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let (ns_segments, table_fragment) = match &create.column.shape {
			MaybeQualifiedColumnShape::Qualified {
				namespace,
				name,
			} => (
				namespace.iter().map(|n| n.text()).collect::<Vec<&str>>(),
				self.interner.intern_fragment(name),
			),
			_ => (vec![], Fragment::internal("_unknown")),
		};

		let Some(ns) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = match &create.column.shape {
				MaybeQualifiedColumnShape::Qualified {
					namespace,
					..
				} => {
					if let Some(n) = namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(ns_segments.join("::"))
					} else {
						Fragment::internal("default".to_string())
					}
				}
				_ => Fragment::internal("default".to_string()),
			};
			return_error!(namespace_not_found(ns_fragment, &ns_segments.join("::")));
		};

		let namespace_id = match &create.column.shape {
			MaybeQualifiedColumnShape::Qualified {
				namespace,
				..
			} => {
				if let Some(n) = namespace.first() {
					let interned = self.interner.intern_fragment(n);
					interned.with_text(ns.name())
				} else {
					Fragment::internal(ns.name().to_string())
				}
			}
			_ => Fragment::internal(ns.name().to_string()),
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, ns);

		Ok(PhysicalPlan::CreateColumnProperty(CreateColumnPropertyNode {
			namespace: resolved_namespace,
			table: table_fragment,
			column: self.interner.intern_fragment(&create.column.name),
			properties: create.properties,
		}))
	}
}
