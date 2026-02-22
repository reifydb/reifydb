// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::catalog::{
	dictionary_not_found, flow_not_found, namespace_not_found, ringbuffer_not_found, sumtype_not_found,
	table_not_found, view_not_found,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	nodes,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_drop_namespace(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropNamespaceNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let full_name: String = drop.segments.iter().map(|s| s.text()).collect::<Vec<_>>().join(".");
		let namespace_name = self.interner.intern_fragment(drop.segments.last().unwrap()).with_text(&full_name);

		match self.catalog.find_namespace_by_name(rx, &full_name)? {
			Some(def) => Ok(PhysicalPlan::DropNamespace(nodes::DropNamespaceNode {
				namespace_name,
				namespace_id: Some(def.id),
				if_exists: drop.if_exists,
				cascade: drop.cascade,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropNamespace(nodes::DropNamespaceNode {
				namespace_name,
				namespace_id: None,
				if_exists: true,
				cascade: drop.cascade,
			})),
			None => {
				return_error!(namespace_not_found(namespace_name, &full_name));
			}
		}
	}

	pub(crate) fn compile_drop_table(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropTableNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if drop.table.namespace.is_empty() {
			"default".to_string()
		} else {
			drop.table.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = drop.table.namespace.first() {
				self.interner.intern_fragment(n).with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let table_name = self.interner.intern_fragment(&drop.table.name);
		let ns_fragment = if let Some(n) = drop.table.namespace.first() {
			self.interner.intern_fragment(n).with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};

		match self.catalog.find_table_by_name(rx, namespace_def.id, drop.table.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropTable(nodes::DropTableNode {
				namespace_name: ns_fragment,
				table_name,
				table_id: Some(def.id),
				if_exists: drop.if_exists,
				cascade: drop.cascade,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropTable(nodes::DropTableNode {
				namespace_name: ns_fragment,
				table_name,
				table_id: None,
				if_exists: true,
				cascade: drop.cascade,
			})),
			None => {
				return_error!(table_not_found(table_name, &namespace_def.name, drop.table.name.text()));
			}
		}
	}

	pub(crate) fn compile_drop_view(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropViewNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if drop.view.namespace.is_empty() {
			"default".to_string()
		} else {
			drop.view.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = drop.view.namespace.first() {
				self.interner.intern_fragment(n).with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let view_name = self.interner.intern_fragment(&drop.view.name);
		let ns_fragment = if let Some(n) = drop.view.namespace.first() {
			self.interner.intern_fragment(n).with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};

		match self.catalog.find_view_by_name(rx, namespace_def.id, drop.view.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropView(nodes::DropViewNode {
				namespace_name: ns_fragment,
				view_name,
				view_id: Some(def.id),
				if_exists: drop.if_exists,
				cascade: drop.cascade,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropView(nodes::DropViewNode {
				namespace_name: ns_fragment,
				view_name,
				view_id: None,
				if_exists: true,
				cascade: drop.cascade,
			})),
			None => {
				return_error!(view_not_found(view_name, &namespace_def.name, drop.view.name.text()));
			}
		}
	}

	pub(crate) fn compile_drop_ringbuffer(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropRingBufferNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if drop.ringbuffer.namespace.is_empty() {
			"default".to_string()
		} else {
			drop.ringbuffer.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = drop.ringbuffer.namespace.first() {
				self.interner.intern_fragment(n).with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let rb_name = self.interner.intern_fragment(&drop.ringbuffer.name);
		let ns_fragment = if let Some(n) = drop.ringbuffer.namespace.first() {
			self.interner.intern_fragment(n).with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};

		match self.catalog.find_ringbuffer_by_name(rx, namespace_def.id, drop.ringbuffer.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropRingBuffer(nodes::DropRingBufferNode {
				namespace_name: ns_fragment,
				ringbuffer_name: rb_name,
				ringbuffer_id: Some(def.id),
				if_exists: drop.if_exists,
				cascade: drop.cascade,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropRingBuffer(nodes::DropRingBufferNode {
				namespace_name: ns_fragment,
				ringbuffer_name: rb_name,
				ringbuffer_id: None,
				if_exists: true,
				cascade: drop.cascade,
			})),
			None => {
				return_error!(ringbuffer_not_found(
					rb_name,
					&namespace_def.name,
					drop.ringbuffer.name.text()
				));
			}
		}
	}

	pub(crate) fn compile_drop_dictionary(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropDictionaryNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if drop.dictionary.namespace.is_empty() {
			"default".to_string()
		} else {
			drop.dictionary.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = drop.dictionary.namespace.first() {
				self.interner.intern_fragment(n).with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let dict_name = self.interner.intern_fragment(&drop.dictionary.name);
		let ns_fragment = if let Some(n) = drop.dictionary.namespace.first() {
			self.interner.intern_fragment(n).with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};

		match self.catalog.find_dictionary_by_name(rx, namespace_def.id, drop.dictionary.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropDictionary(nodes::DropDictionaryNode {
				namespace_name: ns_fragment,
				dictionary_name: dict_name,
				dictionary_id: Some(def.id),
				if_exists: drop.if_exists,
				cascade: drop.cascade,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropDictionary(nodes::DropDictionaryNode {
				namespace_name: ns_fragment,
				dictionary_name: dict_name,
				dictionary_id: None,
				if_exists: true,
				cascade: drop.cascade,
			})),
			None => {
				return_error!(dictionary_not_found(
					dict_name,
					&namespace_def.name,
					drop.dictionary.name.text()
				));
			}
		}
	}

	pub(crate) fn compile_drop_sumtype(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropSumTypeNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if drop.sumtype.namespace.is_empty() {
			"default".to_string()
		} else {
			drop.sumtype.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = drop.sumtype.namespace.first() {
				self.interner.intern_fragment(n).with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let sumtype_name = self.interner.intern_fragment(&drop.sumtype.name);
		let ns_fragment = if let Some(n) = drop.sumtype.namespace.first() {
			self.interner.intern_fragment(n).with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};

		match self.catalog.find_sumtype_by_name(rx, namespace_def.id, drop.sumtype.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropSumType(nodes::DropSumTypeNode {
				namespace_name: ns_fragment,
				sumtype_name,
				sumtype_id: Some(def.id),
				if_exists: drop.if_exists,
				cascade: drop.cascade,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropSumType(nodes::DropSumTypeNode {
				namespace_name: ns_fragment,
				sumtype_name,
				sumtype_id: None,
				if_exists: true,
				cascade: drop.cascade,
			})),
			None => {
				return_error!(sumtype_not_found(
					sumtype_name,
					&namespace_def.name,
					drop.sumtype.name.text()
				));
			}
		}
	}

	pub(crate) fn compile_drop_flow(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropFlowNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if drop.flow.namespace.is_empty() {
			"default".to_string()
		} else {
			drop.flow.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = drop.flow.namespace.first() {
				self.interner.intern_fragment(n).with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let flow_name = self.interner.intern_fragment(&drop.flow.name);
		let ns_fragment = if let Some(n) = drop.flow.namespace.first() {
			self.interner.intern_fragment(n).with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};

		match self.catalog.find_flow_by_name(rx, namespace_def.id, drop.flow.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropFlow(nodes::DropFlowNode {
				namespace_name: ns_fragment,
				flow_name,
				flow_id: Some(def.id),
				if_exists: drop.if_exists,
				cascade: drop.cascade,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropFlow(nodes::DropFlowNode {
				namespace_name: ns_fragment,
				flow_name,
				flow_id: None,
				if_exists: true,
				cascade: drop.cascade,
			})),
			None => {
				return_error!(flow_not_found(flow_name, &namespace_def.name, drop.flow.name.text()));
			}
		}
	}

	pub(crate) fn compile_drop_subscription(
		&mut self,
		_rx: &mut Transaction<'_>,
		drop: logical::DropSubscriptionNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let subscription_name = self.interner.intern_fragment(&drop.identifier);

		// Subscriptions are looked up by ID at execution time, not by name in a namespace.
		// We pass the name through and let the VM handler resolve it.
		Ok(PhysicalPlan::DropSubscription(nodes::DropSubscriptionNode {
			subscription_name,
			if_exists: drop.if_exists,
			cascade: drop.cascade,
		}))
	}
}
