// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, num::NonZeroU64, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::object::ObjectId, change::Change, resolved::ResolvedSeries},
	internal_err,
};
use reifydb_evaluate::stack::SymbolTable;
#[cfg(feature = "testing")]
use reifydb_flow::backfill::testing::{InstalledScanHooks, TestingScan};
use reifydb_flow::backfill::{Scan, backfill};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, params::Params};

use crate::{
	Result,
	vm::{
		services::Services,
		volcano::{
			query::{QueryContext, QueryNode, query_budget},
			scan::{
				ringbuffer::RingBufferScan, series::SeriesScanNode, table::TableScanNode,
				view::ViewScanNode,
			},
		},
	},
};

#[cfg(test)]
mod tests;

pub fn run<'a>(
	services: &Arc<Services>,
	tx: Transaction<'a>,
	sources: &BTreeSet<ObjectId>,
	batch_size: NonZeroU64,
	mut consume: impl FnMut(&mut TransactionScan<'a>, Change) -> Result<()>,
) -> Result<CommitVersion> {
	let mut scan = TransactionScan {
		services: Arc::clone(services),
		tx,
		opened: None,
	};
	let version = scan.version();
	#[cfg(feature = "testing")]
	if let Some(InstalledScanHooks(hooks)) = services.ioc.try_resolve::<InstalledScanHooks>() {
		backfill(&mut TestingScan::over(scan, hooks), sources, batch_size, |testing, change| {
			consume(testing.scan_mut(), change)
		})?;
		return Ok(version);
	}
	backfill(&mut scan, sources, batch_size, &mut consume)?;
	Ok(version)
}

pub struct TransactionScan<'a> {
	services: Arc<Services>,
	tx: Transaction<'a>,
	opened: Option<Opened>,
}

struct Opened {
	node: Box<dyn QueryNode>,
	context: QueryContext,
	tagged_series: bool,
}

impl<'a> TransactionScan<'a> {
	pub fn transaction(&mut self) -> &mut Transaction<'a> {
		&mut self.tx
	}

	fn node(&mut self, source: ObjectId, context: &Arc<QueryContext>) -> Result<(Box<dyn QueryNode>, bool)> {
		let catalog = &self.services.catalog;
		let tx = &mut self.tx;
		match source {
			ObjectId::Table(id) => {
				let table = catalog.resolve_table(tx, id)?;
				let node = TableScanNode::new(table, None, Arc::clone(context), tx)?.oldest_first();
				Ok((Box::new(node), false))
			}
			ObjectId::View(id) => {
				let view = catalog.resolve_view(tx, id)?;
				let node = ViewScanNode::new(view, None, Arc::clone(context), tx)?.oldest_first();
				Ok((Box::new(node), false))
			}
			ObjectId::RingBuffer(id) => {
				let ringbuffer = catalog.resolve_ringbuffer(tx, id)?;
				let node = RingBufferScan::new(ringbuffer, Arc::clone(context), tx)?.oldest_first();
				Ok((Box::new(node), false))
			}
			ObjectId::Series(id) => {
				let series = catalog.get_series(tx, id)?;
				let namespace = catalog.resolve_namespace(tx, series.namespace)?;
				let tagged = series.tag.is_some();
				let series =
					ResolvedSeries::new(Fragment::internal(series.name.clone()), namespace, series);
				let node = SeriesScanNode::new(series, None, None, None, None, Arc::clone(context))?
					.oldest_first();
				Ok((Box::new(node), tagged))
			}
			ObjectId::TableVirtual(_) | ObjectId::Dictionary(_) | ObjectId::Queue(_) => {
				internal_err!("backfill cannot scan source {:?}: it has no row scan node", source)
			}
		}
	}
}

impl Scan for TransactionScan<'_> {
	fn version(&self) -> CommitVersion {
		self.tx.version()
	}

	fn open(&mut self, source: ObjectId, batch_size: NonZeroU64) -> Result<()> {
		let context = Arc::new(QueryContext {
			services: Arc::clone(&self.services),
			source: None,
			batch_size: batch_size.get(),
			params: Params::None,
			symbols: SymbolTable::new(),
			identity: self.tx.identity(),
			memory: query_budget(&self.services),
		});
		let (mut node, tagged_series) = self.node(source, &context)?;
		node.initialize(&mut self.tx, &context)?;
		self.opened = Some(Opened {
			node,
			context: (*context).clone(),
			tagged_series,
		});
		Ok(())
	}

	fn next(&mut self) -> Result<Option<RecordBatch>> {
		let Some(opened) = self.opened.as_mut() else {
			return internal_err!("transaction scan pulled before any source was opened");
		};
		let Some(mut chunk) = opened.node.next(&mut self.tx, &mut opened.context)? else {
			return Ok(None);
		};
		if opened.tagged_series {
			chunk.remove_column(1);
		}
		Ok(Some(chunk))
	}
}
