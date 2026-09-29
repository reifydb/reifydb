// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	flow::dag::FlowDag,
	interface::{
		catalog::{
			dictionary::Dictionary,
			id::{TableId, ViewId},
			object::ObjectId,
			table::Table,
			view::{View, ViewKind},
		},
		change::{Change, ChangeOrigin, Diff},
	},
	key::any::TaggedKey,
};
use reifydb_flow_sync::{
	run::run,
	txn::{Changes, ClockNow, Emit, Intern, Lookup, Rows},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{
	Value,
	datetime::DateTime,
	dictionary::{DictionaryEntryId, DictionaryId},
};
use smallvec::smallvec;

use crate::{Result, transaction::operation::dictionary::DictionaryOperations, vm::services::Services};

pub(crate) fn sync_transactional_views(services: &Services, tx: Transaction<'_>) -> Result<()> {
	let mut txn = FlowTransaction {
		tx,
		catalog: &services.catalog,
		runtime_context: &services.runtime_context,
	};
	run(&mut txn, &services.routines, &services.runtime_context)
}

pub(crate) struct FlowTransaction<'a> {
	tx: Transaction<'a>,
	catalog: &'a Catalog,
	runtime_context: &'a RuntimeContext,
}

fn tagged(key: &EncodedKey) -> TaggedKey {
	TaggedKey::decode(key).unwrap_or_else(|| panic!("flow sync produced an undecodable key {key:?}"))
}

fn load_transactional_flows(catalog: &Catalog, tx: &mut Transaction<'_>) -> Result<Vec<FlowDag>> {
	let mut flows = Vec::new();
	for dag in catalog.list_flow_dags_asc(tx)? {
		if is_transactional(catalog, tx, &dag)? {
			flows.push(dag);
		}
	}
	Ok(flows)
}

fn is_transactional(catalog: &Catalog, tx: &mut Transaction<'_>, dag: &FlowDag) -> Result<bool> {
	for view in dag.sink_views() {
		if catalog.find_view(tx, view)?.is_some_and(|def| def.kind() == ViewKind::Transactional) {
			return Ok(true);
		}
	}
	Ok(false)
}

impl Changes for FlowTransaction<'_> {
	fn cursor(&self) -> usize {
		self.tx.flow_cursor()
	}

	fn entries_from(&self, at: usize) -> Vec<(ObjectId, Diff)> {
		self.tx.flow_entries_from(at).to_vec()
	}

	fn set_cursor(&mut self, at: usize) {
		self.tx.set_flow_cursor(at)
	}
}

impl Rows for FlowTransaction<'_> {
	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		Ok(self.tx.get(&tagged(key))?.map(|row| row.bytes))
	}

	fn set(&mut self, key: &EncodedKey, row: EncodedBytes) -> Result<()> {
		self.tx.set(&tagged(key), row)
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.tx.remove(&tagged(key))
	}
}

impl Emit for FlowTransaction<'_> {
	fn emit(&mut self, view: ViewId, diff: Diff) -> Result<()> {
		self.tx.track_flow_change(Change {
			origin: ChangeOrigin::Object(ObjectId::view(view)),
			diffs: smallvec![diff],
			version: ChangeVersion::from(CommitVersion(0)),
			changed_at: DateTime::default(),
		});
		Ok(())
	}
}

impl Lookup for FlowTransaction<'_> {
	fn transactional_flows(&mut self) -> Result<Vec<FlowDag>> {
		load_transactional_flows(self.catalog, &mut self.tx)
	}

	fn view(&mut self, id: ViewId) -> Result<View> {
		self.catalog.get_view(&mut self.tx, id)
	}

	fn table(&mut self, id: TableId) -> Result<Table> {
		self.catalog.get_table(&mut self.tx, id)
	}

	fn dictionary(&mut self, id: DictionaryId) -> Result<Dictionary> {
		self.catalog.get_dictionary(&mut self.tx, id)
	}
}

impl Intern for FlowTransaction<'_> {
	fn intern(&mut self, dictionary: &Dictionary, value: &Value) -> Result<DictionaryEntryId> {
		self.tx.insert_into_dictionary(dictionary, value)
	}

	fn find(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>> {
		self.tx.find_in_dictionary(dictionary, value)
	}

	fn resolve(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		self.tx.get_from_dictionary(dictionary, id)
	}
}

impl ClockNow for FlowTransaction<'_> {
	fn now(&self) -> DateTime {
		self.runtime_context.clock.now()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{
			catalog::{id::ViewId, object::ObjectId},
			change::Diff,
		},
		value::batch::single_row,
	};
	use reifydb_flow_sync::txn::{Changes, Emit, Lookup};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
	use reifydb_value::{params::Params, value::Value};

	use super::FlowTransaction;
	use crate::vm::{Admin, executor::Executor};

	fn admin(executor: &Executor, txn: &mut AdminTransaction, rql: &str) {
		let r = executor.admin(
			txn,
			Admin {
				rql,
				params: Params::default(),
			},
		);
		if let Some(e) = r.error {
			panic!("{rql}: {e:?}");
		}
	}

	#[test]
	fn an_emitted_view_diff_lands_after_the_cursor_and_moving_the_cursor_skips_it() {
		// Without this the run loop either re-feeds its own view diffs or never hands them to a downstream flow.
		let executor = Executor::testing();
		let mut txn = create_test_admin_transaction();
		let mut sync = FlowTransaction {
			tx: Transaction::Admin(&mut txn),
			catalog: &executor.catalog,
			runtime_context: &executor.runtime_context,
		};
		let at = sync.cursor();
		let diff = Diff::insert(single_row([("id", Value::Int4(1))]).unwrap());
		sync.emit(ViewId(7), diff.clone()).unwrap();
		assert_eq!(sync.entries_from(at), vec![(ObjectId::view(ViewId(7)), diff)]);
		sync.set_cursor(at + 1);
		assert!(sync.entries_from(sync.cursor()).is_empty());
	}

	#[test]
	fn a_transactional_view_created_in_the_same_txn_is_listed_and_a_deferred_one_is_not() {
		// A create-then-write txn must maintain the new view; a deferred flow here would be run twice.
		let executor = Executor::testing();
		let mut txn = create_test_admin_transaction();
		admin(&executor, &mut txn, "CREATE NAMESPACE ns");
		admin(&executor, &mut txn, "CREATE TABLE ns::src { id: int4 }");
		admin(&executor, &mut txn, "CREATE DEFERRED VIEW ns::d { id: int4 } AS { FROM ns::src }");
		admin(&executor, &mut txn, "CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::src }");
		let mut sync = FlowTransaction {
			tx: Transaction::Admin(&mut txn),
			catalog: &executor.catalog,
			runtime_context: &executor.runtime_context,
		};
		let flows = sync.transactional_flows().unwrap();
		assert_eq!(flows.len(), 1, "only the transactional view's flow may be listed");
	}
}
