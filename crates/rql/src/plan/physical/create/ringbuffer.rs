// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use PhysicalPlan::CreateRingBuffer;
use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::plan::{
	logical,
	physical::{Compiler, CreateRingBufferNode, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_ringbuffer<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		create: logical::CreateRingBufferNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		// Get namespace name from the MaybeQualified type (join all segments for nested namespaces)
		let namespace_name = if create.ringbuffer.namespace.is_empty() {
			"default".to_string()
		} else {
			create.ringbuffer.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.ringbuffer.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let namespace_id = if let Some(n) = create.ringbuffer.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		Ok(CreateRingBuffer(CreateRingBufferNode {
			namespace: resolved_namespace,
			ringbuffer: self.interner.intern_fragment(&create.ringbuffer.name),
			if_not_exists: create.if_not_exists,
			columns: create.columns,
			capacity: create.capacity,
			primary_key: super::materialize_primary_key(&mut self.interner, create.primary_key),
		}))
	}
}
