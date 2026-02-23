// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::interface::catalog::sumtype::SumTypeKind;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	nodes::CreateHandlerNode,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_handler(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateHandlerNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		// Resolve namespace for the handler itself (from the handler name)
		let handler_ns_name = if create.name.namespace.is_empty() {
			"default".to_string()
		} else {
			create.name.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &handler_ns_name)? else {
			let ns_fragment = if let Some(n) = create.name.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&handler_ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: handler_ns_name.to_string(),
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
		};

		// Resolve event sumtype namespace
		let event_ns_name = if create.on_event.namespace.is_empty() {
			handler_ns_name.clone()
		} else {
			create.on_event.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(event_ns_def) = self.catalog.find_namespace_by_name(rx, &event_ns_name)? else {
			let ns_fragment = Fragment::internal(event_ns_name.clone());
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: event_ns_name.to_string(),
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
		};

		// Look up the event sumtype by name
		let event_name = create.on_event.name.text();
		let Some(sumtype_def) = self.catalog.find_sumtype_by_name(rx, event_ns_def.id, event_name)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Event,
				namespace: event_ns_name.to_string(),
				name: event_name.to_string(),
				fragment: Fragment::internal(event_name.to_string()),
			}
			.into());
		};

		// Verify it's an event type
		if sumtype_def.kind != SumTypeKind::Event {
			return Err(reifydb_core::internal_error!(
				"'{}' is not an EVENT type. Use CREATE EVENT to declare event types.",
				event_name
			));
		}

		// Find variant by name → get tag
		let variant_name = create.on_variant.text().to_lowercase();
		let Some(variant_def) = sumtype_def.variants.iter().find(|v| v.name == variant_name) else {
			return Err(reifydb_core::internal_error!(
				"Variant '{}' not found in event type '{}'",
				create.on_variant.text(),
				event_name
			));
		};

		let on_variant_tag = variant_def.tag;

		Ok(PhysicalPlan::CreateHandler(CreateHandlerNode {
			namespace: namespace_def,
			name: self.interner.intern_fragment(&create.name.name),
			on_sumtype_id: sumtype_def.id,
			on_variant_tag,
			body_source: create.body_source,
		}))
	}
}
