// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
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

#[async_trait]
impl PostCommitInterceptor<StandardCommandTransaction> for CatalogEventInterceptor {
	async fn intercept(&self, ctx: &mut PostCommitContext) -> crate::Result<()> {
		// Emit events for namespace changes
		for change in &ctx.changes.namespace_def {
			if change.op == OperationType::Create {
				if let Some(namespace) = &change.post {
					self.event_bus
						.emit(NamespaceCreatedEvent {
							namespace: namespace.clone(),
						})
						.await;
				}
			}
		}

		// Emit events for table definition changes
		for change in &ctx.changes.table_def {
			if change.op == OperationType::Create {
				if let Some(table) = &change.post {
					self.event_bus
						.emit(TableCreatedEvent {
							table: table.clone(),
						})
						.await;
				}
			}
		}

		// Emit events for view changes
		for change in &ctx.changes.view_def {
			if change.op == OperationType::Create {
				if let Some(view) = &change.post {
					self.event_bus
						.emit(ViewCreatedEvent {
							view: view.clone(),
						})
						.await;
				}
			}
		}

		// Emit events for ring buffer changes
		for change in &ctx.changes.ringbuffer_def {
			if change.op == OperationType::Create {
				if let Some(ringbuffer) = &change.post {
					self.event_bus
						.emit(RingBufferCreatedEvent {
							ringbuffer: ringbuffer.clone(),
						})
						.await;
				}
			}
		}

		// Emit events for dictionary changes
		for change in &ctx.changes.dictionary_def {
			if change.op == OperationType::Create {
				if let Some(dictionary) = &change.post {
					self.event_bus
						.emit(DictionaryCreatedEvent {
							dictionary: dictionary.clone(),
						})
						.await;
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
						let layout = table.get_named_layout();
						let row = Row {
							number: insertion.row_number,
							encoded: insertion.encoded.clone(),
							layout,
						};
						self.event_bus
							.emit(TableInsertedEvent {
								table,
								row,
							})
							.await;
					}
				} // Future: handle other RowChange variants
			}
		}

		Ok(())
	}
}
