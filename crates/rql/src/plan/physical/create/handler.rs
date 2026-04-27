// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{
	interface::catalog::{procedure::RqlTrigger, sumtype::SumTypeKind},
	internal_error,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::sumtype::VariantRef};

use crate::{
	Result,
	nodes::CreateProcedureNode,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_handler(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateProcedureNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		// Handler-style procedures always have on_event/on_variant set
		let on_event = create.on_event.expect("handler must have on_event");
		let on_variant = create.on_variant.expect("handler must have on_variant");

		// Resolve namespace for the handler itself (from the handler name)
		let handler_ns_segments: Vec<&str> = create.procedure.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &handler_ns_segments)? else {
			let ns_fragment = if let Some(n) = create.procedure.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(handler_ns_segments.join("::"))
			} else {
				Fragment::internal("default")
			};
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: handler_ns_segments.join("::"),
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
		};

		// Resolve event sumtype namespace
		let event_ns_segments: Vec<&str> = if on_event.namespace.is_empty() {
			handler_ns_segments.clone()
		} else {
			on_event.namespace.iter().map(|n| n.text()).collect()
		};
		let Some(event_ns_def) = self.catalog.find_namespace_by_segments(rx, &event_ns_segments)? else {
			let ns_fragment = Fragment::internal(event_ns_segments.join("::"));
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: event_ns_segments.join("::"),
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
		};

		// Look up the event sumtype by name
		let event_name = on_event.name.text();
		let Some(sumtype) = self.catalog.find_sumtype_by_name(rx, event_ns_def.id(), event_name)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Event,
				namespace: event_ns_segments.join("::"),
				name: event_name.to_string(),
				fragment: Fragment::internal(event_name),
			}
			.into());
		};

		// Verify it's an event type
		if sumtype.kind != SumTypeKind::Event {
			return Err(internal_error!(
				"'{}' is not an EVENT type. Use CREATE EVENT to declare event types.",
				event_name
			));
		}

		// Find variant by name → get tag
		let variant_name = on_variant.text().to_lowercase();
		let Some(variant) = sumtype.variants.iter().find(|v| v.name == variant_name) else {
			return Err(internal_error!(
				"Variant '{}' not found in event type '{}'",
				on_variant.text(),
				event_name
			));
		};

		Ok(PhysicalPlan::CreateProcedure(CreateProcedureNode {
			namespace,
			name: self.interner.intern_fragment(&create.procedure.name),
			params: vec![],
			body_source: create.body_source,
			trigger: RqlTrigger::Event {
				variant: VariantRef {
					sumtype_id: sumtype.id,
					variant_tag: variant.tag,
				},
			},
			is_test: false,
		}))
	}
}
