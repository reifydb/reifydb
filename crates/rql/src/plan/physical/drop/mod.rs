// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::catalog::{
	dictionary_not_found, handler_not_found, namespace_not_found, procedure_not_found, ringbuffer_not_found,
	series_not_found, sumtype_not_found, table_not_found, test_not_found, view_not_found,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	Result, nodes,
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
	) -> Result<PhysicalPlan<'bump>> {
		let full_name: String = drop.segments.iter().map(|s| s.text()).collect::<Vec<_>>().join("::");
		let ns_segments: Vec<&str> = drop.segments.iter().map(|s| s.text()).collect();
		let namespace_name = self.interner.intern_fragment(drop.segments.last().unwrap()).with_text(&full_name);

		match self.catalog.find_namespace_by_segments(rx, &ns_segments)? {
			Some(def) => Ok(PhysicalPlan::DropNamespace(nodes::DropNamespaceNode {
				namespace_name,
				namespace_id: Some(def.id()),
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
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.table.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.table.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let table_name = self.interner.intern_fragment(&drop.table.name);
		let ns_fragment = if let Some(n) = drop.table.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		match self.catalog.find_table_by_name(rx, namespace.id(), drop.table.name.text())? {
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
				return_error!(table_not_found(table_name, namespace.name(), drop.table.name.text()));
			}
		}
	}

	pub(crate) fn compile_drop_view(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropViewNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.view.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.view.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let view_name = self.interner.intern_fragment(&drop.view.name);
		let ns_fragment = if let Some(n) = drop.view.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		match self.catalog.find_view_by_name(rx, namespace.id(), drop.view.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropView(nodes::DropViewNode {
				namespace_name: ns_fragment,
				view_name,
				view_id: Some(def.id()),
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
				return_error!(view_not_found(view_name, namespace.name(), drop.view.name.text()));
			}
		}
	}

	pub(crate) fn compile_drop_ringbuffer(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropRingBufferNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.ringbuffer.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.ringbuffer.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let rb_name = self.interner.intern_fragment(&drop.ringbuffer.name);
		let ns_fragment = if let Some(n) = drop.ringbuffer.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		match self.catalog.find_ringbuffer_by_name(rx, namespace.id(), drop.ringbuffer.name.text())? {
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
					namespace.name(),
					drop.ringbuffer.name.text()
				));
			}
		}
	}

	pub(crate) fn compile_drop_dictionary(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropDictionaryNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.dictionary.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.dictionary.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let dict_name = self.interner.intern_fragment(&drop.dictionary.name);
		let ns_fragment = if let Some(n) = drop.dictionary.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		match self.catalog.find_dictionary_by_name(rx, namespace.id(), drop.dictionary.name.text())? {
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
					namespace.name(),
					drop.dictionary.name.text()
				));
			}
		}
	}

	pub(crate) fn compile_drop_sumtype(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropSumTypeNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.sumtype.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.sumtype.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let sumtype_name = self.interner.intern_fragment(&drop.sumtype.name);
		let ns_fragment = if let Some(n) = drop.sumtype.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		match self.catalog.find_sumtype_by_name(rx, namespace.id(), drop.sumtype.name.text())? {
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
					namespace.name(),
					drop.sumtype.name.text()
				));
			}
		}
	}

	pub(crate) fn compile_drop_subscription(
		&mut self,
		_rx: &mut Transaction<'_>,
		drop: logical::DropSubscriptionNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let subscription_name = self.interner.intern_fragment(&drop.identifier);

		// Subscriptions are looked up by ID at execution time, not by name in a namespace.
		// We pass the name through and let the VM handler resolve it.
		Ok(PhysicalPlan::DropSubscription(nodes::DropSubscriptionNode {
			subscription_name,
			if_exists: drop.if_exists,
			cascade: drop.cascade,
		}))
	}

	pub(crate) fn compile_drop_series(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropSeriesNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.series.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.series.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let series_name = self.interner.intern_fragment(&drop.series.name);
		let ns_fragment = if let Some(n) = drop.series.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		match self.catalog.find_series_by_name(rx, namespace.id(), drop.series.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropSeries(nodes::DropSeriesNode {
				namespace_name: ns_fragment,
				series_name,
				series_id: Some(def.id),
				if_exists: drop.if_exists,
				cascade: drop.cascade,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropSeries(nodes::DropSeriesNode {
				namespace_name: ns_fragment,
				series_name,
				series_id: None,
				if_exists: true,
				cascade: drop.cascade,
			})),
			None => {
				return_error!(series_not_found(series_name, namespace.name(), drop.series.name.text()));
			}
		}
	}

	pub(crate) fn compile_drop_source(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropSourceNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.source.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.source.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		Ok(PhysicalPlan::DropSource(nodes::DropSourceNode {
			if_exists: drop.if_exists,
			namespace,
			name: self.interner.intern_fragment(&drop.source.name),
			cascade: drop.cascade,
		}))
	}

	pub(crate) fn compile_drop_procedure(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropProcedureNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.procedure.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.procedure.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let procedure_name = self.interner.intern_fragment(&drop.procedure.name);
		let ns_fragment = if let Some(n) = drop.procedure.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		match self.catalog.find_procedure_by_name(rx, namespace.id(), drop.procedure.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropProcedure(nodes::DropProcedureNode {
				namespace_name: ns_fragment,
				procedure_name,
				procedure_id: Some(def.id()),
				if_exists: drop.if_exists,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropProcedure(nodes::DropProcedureNode {
				namespace_name: ns_fragment,
				procedure_name,
				procedure_id: None,
				if_exists: true,
			})),
			None => {
				return_error!(procedure_not_found(
					procedure_name,
					namespace.name(),
					drop.procedure.name.text()
				));
			}
		}
	}

	pub(crate) fn compile_drop_sink(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropSinkNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.sink.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.sink.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		Ok(PhysicalPlan::DropSink(nodes::DropSinkNode {
			if_exists: drop.if_exists,
			namespace,
			name: self.interner.intern_fragment(&drop.sink.name),
			cascade: drop.cascade,
		}))
	}

	pub(crate) fn compile_drop_handler(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropHandlerNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.handler.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.handler.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let handler_name = self.interner.intern_fragment(&drop.handler.name);
		let ns_fragment = if let Some(n) = drop.handler.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		let procedure_opt =
			self.catalog.find_procedure_by_name(rx, namespace.id(), drop.handler.name.text())?;
		let handler_opt = self.catalog.find_handler_by_name(rx, namespace.id(), drop.handler.name.text())?;

		match (procedure_opt, handler_opt) {
			(procedure, handler) if procedure.is_some() || handler.is_some() => {
				Ok(PhysicalPlan::DropHandler(nodes::DropHandlerNode {
					namespace_name: ns_fragment,
					handler_name,
					procedure_id: procedure.map(|p| p.id()),
					handler_id: handler.map(|h| h.id),
					if_exists: drop.if_exists,
				}))
			}
			_ if drop.if_exists => Ok(PhysicalPlan::DropHandler(nodes::DropHandlerNode {
				namespace_name: ns_fragment,
				handler_name,
				procedure_id: None,
				handler_id: None,
				if_exists: true,
			})),
			_ => {
				return_error!(handler_not_found(
					handler_name,
					namespace.name(),
					drop.handler.name.text()
				));
			}
		}
	}

	pub(crate) fn compile_drop_test(
		&mut self,
		rx: &mut Transaction<'_>,
		drop: logical::DropTestNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = drop.test.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let ns_fragment = if let Some(n) = drop.test.namespace.first() {
				self.interner.intern_fragment(n).with_text(&ns_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &ns_name));
		};

		let test_name = self.interner.intern_fragment(&drop.test.name);
		let ns_fragment = if let Some(n) = drop.test.namespace.first() {
			self.interner.intern_fragment(n).with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name().to_string())
		};

		match self.catalog.find_test_by_name(rx, namespace.id(), drop.test.name.text())? {
			Some(def) => Ok(PhysicalPlan::DropTest(nodes::DropTestNode {
				namespace_name: ns_fragment,
				test_name,
				test_id: Some(def.id),
				if_exists: drop.if_exists,
			})),
			None if drop.if_exists => Ok(PhysicalPlan::DropTest(nodes::DropTestNode {
				namespace_name: ns_fragment,
				test_name,
				test_id: None,
				if_exists: true,
			})),
			None => {
				return_error!(test_not_found(test_name, namespace.name(), drop.test.name.text()));
			}
		}
	}
}
