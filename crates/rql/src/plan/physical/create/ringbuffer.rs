// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateRingBuffer;
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	diagnostic::catalog::namespace_not_found,
	interface::{QueryTransaction, resolved::ResolvedNamespace},
};
use reifydb_type::return_error;

use crate::plan::{
	logical,
	physical::{Compiler, CreateRingBufferNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_ringbuffer<'a>(
		rx: &mut impl QueryTransaction,
		create: logical::CreateRingBufferNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.ringbuffer.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace_def) = CatalogStore::find_namespace_by_name(rx, namespace_name)? else {
			let ns_fragment = create.ringbuffer.namespace.clone().unwrap_or_else(|| {
				use reifydb_type::Fragment;
				Fragment::owned_internal("default".to_string())
			});
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		// Create a ResolvedNamespace
		let namespace_id = create.ringbuffer.namespace.clone().unwrap_or_else(|| {
			use reifydb_type::Fragment;
			Fragment::owned_internal(namespace_def.name.clone())
		});
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		Ok(CreateRingBuffer(CreateRingBufferNode {
			namespace: resolved_namespace,
			ringbuffer: create.ringbuffer.name.clone(), // Extract just the name Fragment
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			capacity: create.capacity,
			primary_key: create.primary_key,
		}))
	}
}
