// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result,
	ast::identifier::MaybeQualifiedColumnPrimitive,
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
		let (namespace_name, table_fragment) = match &create.column.primitive {
			MaybeQualifiedColumnPrimitive::Primitive {
				namespace,
				primitive,
			} => {
				let ns = if namespace.is_empty() {
					"default".to_string()
				} else {
					namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
				};
				(ns, self.interner.intern_fragment(primitive))
			}
			_ => ("default".to_string(), Fragment::internal("_unknown")),
		};

		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = match &create.column.primitive {
				MaybeQualifiedColumnPrimitive::Primitive {
					namespace,
					..
				} => {
					if let Some(n) = namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_name)
					} else {
						Fragment::internal("default".to_string())
					}
				}
				_ => Fragment::internal("default".to_string()),
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let namespace_id = match &create.column.primitive {
			MaybeQualifiedColumnPrimitive::Primitive {
				namespace,
				..
			} => {
				if let Some(n) = namespace.first() {
					let interned = self.interner.intern_fragment(n);
					interned.with_text(&namespace_def.name)
				} else {
					Fragment::internal(namespace_def.name.clone())
				}
			}
			_ => Fragment::internal(namespace_def.name.clone()),
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		Ok(PhysicalPlan::CreateColumnProperty(CreateColumnPropertyNode {
			namespace: resolved_namespace,
			table: table_fragment,
			column: self.interner.intern_fragment(&create.column.name),
			properties: create.properties,
		}))
	}
}
