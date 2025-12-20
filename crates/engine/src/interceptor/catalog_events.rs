// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	Row,
	event::{
		EventBus,
		catalog::{
			DictionaryCreatedEvent, NamespaceCreatedEvent, RingBufferCreatedEvent, TableCreatedEvent,
			TableInsertedEvent, ViewCreatedEvent,
		},
	},
	interceptor::{PostCommitContext, PostCommitInterceptor},
	interface::{GetEncodedRowNamedLayout, OperationType, RowChange},
};

use crate::transaction::StandardCommandTransaction;

pub(crate) struct CatalogEventInterceptor {
	event_bus: EventBus,
	catalog: MaterializedCatalog,
}

impl CatalogEventInterceptor {
	pub fn new(event_bus: EventBus, catalog: MaterializedCatalog) -> Self {
		Self {
			event_bus,
			catalog,
		}
	}
}

impl PostCommitInterceptor<StandardCommandTransaction> for CatalogEventInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> crate::Result<()> {
		// Get handle if tokio runtime is available
		let handle = tokio::runtime::Handle::try_current().ok();

		// Emit events for namespace changes
		for change in &ctx.changes.namespace_def {
			if change.op == OperationType::Create {
				if let Some(namespace) = &change.post {
					if let Some(handle) = &handle {
						let event_bus = self.event_bus.clone();
						let namespace = namespace.clone();
						handle.spawn(async move {
							event_bus
								.emit(NamespaceCreatedEvent {
									namespace,
								})
								.await;
						});
					}
				}
			}
		}

		// Emit events for table definition changes
		for change in &ctx.changes.table_def {
			if change.op == OperationType::Create {
				if let Some(table) = &change.post {
					if let Some(handle) = &handle {
						let event_bus = self.event_bus.clone();
						let table = table.clone();
						handle.spawn(async move {
							event_bus
								.emit(TableCreatedEvent {
									table,
								})
								.await;
						});
					}
				}
			}
		}

		// Emit events for view changes
		for change in &ctx.changes.view_def {
			if change.op == OperationType::Create {
				if let Some(view) = &change.post {
					if let Some(handle) = &handle {
						let event_bus = self.event_bus.clone();
						let view = view.clone();
						handle.spawn(async move {
							event_bus
								.emit(ViewCreatedEvent {
									view,
								})
								.await;
						});
					}
				}
			}
		}

		// Emit events for ring buffer changes
		for change in &ctx.changes.ringbuffer_def {
			if change.op == OperationType::Create {
				if let Some(ringbuffer) = &change.post {
					if let Some(handle) = &handle {
						let event_bus = self.event_bus.clone();
						let ringbuffer = ringbuffer.clone();
						handle.spawn(async move {
							event_bus
								.emit(RingBufferCreatedEvent {
									ringbuffer,
								})
								.await;
						});
					}
				}
			}
		}

		// Emit events for dictionary changes
		for change in &ctx.changes.dictionary_def {
			if change.op == OperationType::Create {
				if let Some(dictionary) = &change.post {
					if let Some(handle) = &handle {
						let event_bus = self.event_bus.clone();
						let dictionary = dictionary.clone();
						handle.spawn(async move {
							event_bus
								.emit(DictionaryCreatedEvent {
									dictionary,
								})
								.await;
						});
					}
				}
			}
		}

		// Emit events for row changes
		for row_change in &ctx.row_changes {
			match row_change {
				RowChange::TableInsert(insertion) => {
					// First try to find in current transaction changes
					let table = ctx
						.changes
						.table_def
						.iter()
						.find_map(|change| change.post.as_ref().filter(|t| t.id == insertion.table_id))
						.cloned()
						// Fall back to catalog lookup
						.or_else(|| self.catalog.find_table(insertion.table_id, ctx.version));

					if let Some(table) = table {
						if let Some(handle) = &handle {
							let layout = table.get_named_layout();
							let event_bus = self.event_bus.clone();
							let row = Row {
								number: insertion.row_number,
								encoded: insertion.encoded.clone(),
								layout,
							};
							handle.spawn(async move {
								event_bus
									.emit(TableInsertedEvent {
										table,
										row,
									})
									.await;
							});
						}
					}
				} // Future: handle other RowChange variants
			}
		}

		Ok(())
	}
}
