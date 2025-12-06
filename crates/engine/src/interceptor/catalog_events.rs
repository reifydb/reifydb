// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::{
		EventBus,
		catalog::{
			DictionaryCreatedEvent, NamespaceCreatedEvent, RingBufferCreatedEvent, TableCreatedEvent,
			ViewCreatedEvent,
		},
	},
	interceptor::{PostCommitContext, PostCommitInterceptor},
	interface::OperationType,
};

use crate::transaction::StandardCommandTransaction;

pub(crate) struct CatalogEventInterceptor {
	event_bus: EventBus,
}

impl CatalogEventInterceptor {
	pub fn new(event_bus: EventBus) -> Self {
		Self {
			event_bus,
		}
	}
}

impl PostCommitInterceptor<StandardCommandTransaction> for CatalogEventInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> crate::Result<()> {
		// Emit events for namespace changes
		for change in &ctx.changes.namespace_def {
			if change.op == OperationType::Create {
				if let Some(namespace) = &change.post {
					self.event_bus.emit(NamespaceCreatedEvent {
						namespace: namespace.clone(),
					});
				}
			}
		}

		// Emit events for table definition changes
		for change in &ctx.changes.table_def {
			if change.op == OperationType::Create {
				if let Some(table) = &change.post {
					self.event_bus.emit(TableCreatedEvent {
						table: table.clone(),
					});
				}
			}
		}

		// Emit events for view changes
		for change in &ctx.changes.view_def {
			if change.op == OperationType::Create {
				if let Some(view) = &change.post {
					self.event_bus.emit(ViewCreatedEvent {
						view: view.clone(),
					});
				}
			}
		}

		// Emit events for ring buffer changes
		for change in &ctx.changes.ring_buffer_def {
			if change.op == OperationType::Create {
				if let Some(ring_buffer) = &change.post {
					self.event_bus.emit(RingBufferCreatedEvent {
						ring_buffer: ring_buffer.clone(),
					});
				}
			}
		}

		// Emit events for dictionary changes
		for change in &ctx.changes.dictionary_def {
			if change.op == OperationType::Create {
				if let Some(dictionary) = &change.post {
					self.event_bus.emit(DictionaryCreatedEvent {
						dictionary: dictionary.clone(),
					});
				}
			}
		}

		Ok(())
	}
}
