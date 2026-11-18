// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::interface::catalog::namespace::Namespace;
use reifydb_rql_frontend::ast::identifier::MaybeQualifiedDeferredViewIdentifier;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::ValueError, fragment::Fragment};

use crate::plan::{
	logical,
	physical::{Compiler, CreateDeferredWindowedViewGroupNode, PhysicalPlan, WindowedViewGroupMember},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_deferred_windowed_view_group(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateDeferredWindowedViewGroupNode<'bump>,
	) -> Result<PhysicalPlan<'bump>, ValueError> {
		let namespace = self.resolve_view_namespace(rx, &create.group)?;
		let own = format!("{}::{}", namespace.name(), create.group.name.text());
		let mut members = Vec::with_capacity(create.members.len());
		for member in create.members {
			let member_text =
				member.view.namespace.iter().map(|n| n.text()).collect::<Vec<&str>>().join("::");
			let member_namespace = if member_text == own {
				None
			} else {
				Some(self.resolve_view_namespace(rx, &member.view)?)
			};
			let as_clause = self.compile(rx, member.as_clause)?.unwrap();
			members.push(WindowedViewGroupMember {
				namespace: member_namespace,
				view: self.interner.intern_fragment(&member.view.name),
				duration: member.duration,
				ttl: member.ttl,
				persistent: member.persistent,
				ttl_eviction: member.ttl_eviction,
				as_clause: self.bump_box(as_clause),
			});
		}

		Ok(PhysicalPlan::CreateDeferredWindowedViewGroup(CreateDeferredWindowedViewGroupNode {
			namespace,
			group: self.interner.intern_fragment(&create.group.name),
			columns: create.columns,
			storage_kind: create.storage_kind,
			members,
		}))
	}

	fn resolve_view_namespace(
		&mut self,
		rx: &mut Transaction<'_>,
		identifier: &MaybeQualifiedDeferredViewIdentifier<'bump>,
	) -> Result<Namespace, ValueError> {
		let ns_segments: Vec<&str> = identifier.namespace.iter().map(|n| n.text()).collect();
		if let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? {
			return Ok(namespace);
		}
		let ns_fragment = if let Some(n) = identifier.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(ns_segments.join("::"))
		} else {
			Fragment::internal("default")
		};
		Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Namespace,
			namespace: ns_segments.join("::"),
			name: String::new(),
			fragment: ns_fragment,
		}
		.into())
	}
}
