// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::interface::{
	catalog::{
		dictionary::Dictionary,
		id::{TableId, ViewId},
		object::ObjectId,
		table::Table,
		view::View,
	},
	change::Diff,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

use crate::txn::{Changes, ClockNow, Emit, Intern, Lookup, Rows};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continue {
	Yes,
	Crash,
}

#[derive(Debug, PartialEq)]
pub enum Outcome {
	Land,
	Err(Error),
}

#[derive(Debug, PartialEq)]
pub enum LookupOutcome {
	Clean,
	Absent,
	Err(Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupTarget {
	Flows,
	View(ViewId),
	Table(TableId),
	Dictionary(DictionaryId),
}

pub trait SyncHooks: Send + Sync {
	fn on_write(&self, _key: &EncodedKey) -> Outcome {
		Outcome::Land
	}

	fn on_emit(&self, _view: ViewId) -> Outcome {
		Outcome::Land
	}

	fn on_intern(&self, _dictionary: DictionaryId) -> Outcome {
		Outcome::Land
	}

	fn on_resolve(&self, _dictionary: DictionaryId) -> Outcome {
		Outcome::Land
	}

	fn on_lookup(&self, _target: LookupTarget) -> LookupOutcome {
		LookupOutcome::Clean
	}

	fn on_call(&self, _call: u64) -> Continue {
		Continue::Yes
	}
}

pub struct NoFaults;

impl SyncHooks for NoFaults {}

pub struct TestingTxn<T> {
	txn: T,
	hooks: Arc<dyn SyncHooks>,
	calls: u64,
}

impl<T> TestingTxn<T> {
	pub fn over(txn: T, hooks: Arc<dyn SyncHooks>) -> Self {
		Self {
			txn,
			hooks,
			calls: 0,
		}
	}

	pub fn txn(&self) -> &T {
		&self.txn
	}

	pub fn calls(&self) -> u64 {
		self.calls
	}

	fn call(&mut self) {
		self.calls += 1;
		let call = self.calls;
		if self.hooks.on_call(call) == Continue::Crash {
			panic!("simulated crash at call {call}");
		}
	}
}

impl<T: Changes> Changes for TestingTxn<T> {
	fn cursor(&self) -> usize {
		self.txn.cursor()
	}

	fn entries_from(&self, at: usize) -> Vec<(ObjectId, Diff)> {
		self.txn.entries_from(at)
	}

	fn set_cursor(&mut self, at: usize) {
		self.txn.set_cursor(at)
	}
}

impl<T: Rows> Rows for TestingTxn<T> {
	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		self.txn.get(key)
	}

	fn set(&mut self, key: &EncodedKey, row: EncodedBytes) -> Result<()> {
		self.call();
		match self.hooks.on_write(key) {
			Outcome::Land => self.txn.set(key, row),
			Outcome::Err(error) => Err(error),
		}
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.call();
		match self.hooks.on_write(key) {
			Outcome::Land => self.txn.remove(key),
			Outcome::Err(error) => Err(error),
		}
	}
}

impl<T: Emit> Emit for TestingTxn<T> {
	fn emit(&mut self, view: ViewId, diff: Diff) -> Result<()> {
		self.call();
		match self.hooks.on_emit(view) {
			Outcome::Land => self.txn.emit(view, diff),
			Outcome::Err(error) => Err(error),
		}
	}
}

impl<T: Lookup> Lookup for TestingTxn<T> {
	fn transactional_flows(&mut self) -> Result<Vec<FlowDag>> {
		match self.hooks.on_lookup(LookupTarget::Flows) {
			LookupOutcome::Clean => self.txn.transactional_flows(),
			LookupOutcome::Absent => Ok(Vec::new()),
			LookupOutcome::Err(error) => Err(error),
		}
	}

	fn view(&mut self, id: ViewId) -> Result<View> {
		match self.hooks.on_lookup(LookupTarget::View(id)) {
			LookupOutcome::Clean => self.txn.view(id),
			LookupOutcome::Absent => panic!("lookup of view {id} cannot be absent"),
			LookupOutcome::Err(error) => Err(error),
		}
	}

	fn table(&mut self, id: TableId) -> Result<Table> {
		match self.hooks.on_lookup(LookupTarget::Table(id)) {
			LookupOutcome::Clean => self.txn.table(id),
			LookupOutcome::Absent => panic!("lookup of table {id} cannot be absent"),
			LookupOutcome::Err(error) => Err(error),
		}
	}

	fn dictionary(&mut self, id: DictionaryId) -> Result<Dictionary> {
		match self.hooks.on_lookup(LookupTarget::Dictionary(id)) {
			LookupOutcome::Clean => self.txn.dictionary(id),
			LookupOutcome::Absent => panic!("lookup of dictionary {id} cannot be absent"),
			LookupOutcome::Err(error) => Err(error),
		}
	}
}

impl<T: Intern> Intern for TestingTxn<T> {
	fn intern(&mut self, dictionary: &Dictionary, value: &Value) -> Result<DictionaryEntryId> {
		self.call();
		match self.hooks.on_intern(dictionary.id) {
			Outcome::Land => self.txn.intern(dictionary, value),
			Outcome::Err(error) => Err(error),
		}
	}

	fn resolve(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self.hooks.on_resolve(dictionary.id) {
			Outcome::Land => self.txn.resolve(dictionary, id),
			Outcome::Err(error) => Err(error),
		}
	}
}

impl<T: ClockNow> ClockNow for TestingTxn<T> {
	fn now(&self) -> DateTime {
		self.txn.now()
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::BTreeMap,
		panic::{AssertUnwindSafe, catch_unwind},
		sync::Arc,
	};

	use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
	use reifydb_core::{
		common::TimeSource,
		interface::{
			catalog::{
				dictionary::Dictionary,
				flow::FlowId,
				id::{NamespaceId, TableId, ViewId},
				table::Table,
				view::{TableView, View, ViewKind},
			},
			change::Diff,
		},
		value::column::columns::Columns,
	};
	use reifydb_rql::flow::flow::FlowBuilder;
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_value::{
		error::{Diagnostic, Error},
		util::cowvec::CowVec,
		value::{Value, dictionary::DictionaryId, value_type::ValueType},
	};

	use super::{Continue, LookupOutcome, LookupTarget, Outcome, SyncHooks, TestingTxn};
	use crate::{
		memory::MemoryTxn,
		txn::{Changes, ClockNow, Emit, Intern, Lookup, Rows},
	};

	const VIEW: ViewId = ViewId(4);
	const TABLE: TableId = TableId(5);
	const SYMBOLS: DictionaryId = DictionaryId(7);

	fn refused() -> Error {
		Error(Box::new(Diagnostic {
			code: "TEST_REFUSED".to_string(),
			..Default::default()
		}))
	}

	fn key(id: u8) -> EncodedKey {
		EncodedKey::new([id])
	}

	fn row(byte: u8) -> EncodedBytes {
		EncodedBytes(CowVec::new(vec![byte]))
	}

	fn utf8(text: &str) -> Value {
		Value::Utf8(text.to_string())
	}

	fn symbols() -> Dictionary {
		Dictionary {
			id: SYMBOLS,
			namespace: NamespaceId(1),
			name: "symbols".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint2,
		}
	}

	fn positions() -> View {
		View::Table(TableView {
			id: VIEW,
			namespace: NamespaceId(1),
			name: "positions".to_string(),
			kind: ViewKind::Transactional,
			columns: Vec::new(),
			primary_key: None,
			partition_by: Vec::new(),
			sort: Vec::new(),
		})
	}

	fn trades() -> Table {
		Table {
			id: TABLE,
			namespace: NamespaceId(1),
			name: "trades".to_string(),
			columns: Vec::new(),
			primary_key: None,
			partition_by: Vec::new(),
			time: TimeSource::None,
		}
	}

	fn catalog() -> MemoryTxn {
		let mut txn = MemoryTxn::default();
		txn.views.insert(VIEW, positions());
		txn.tables.insert(TABLE, trades());
		txn.dictionaries.insert(SYMBOLS, symbols());
		txn.flows.push(FlowBuilder::new(FlowId(1)).build());
		txn
	}

	struct RefuseWrites;

	impl SyncHooks for RefuseWrites {
		fn on_write(&self, _key: &EncodedKey) -> Outcome {
			Outcome::Err(refused())
		}
	}

	struct RefuseEmits;

	impl SyncHooks for RefuseEmits {
		fn on_emit(&self, _view: ViewId) -> Outcome {
			Outcome::Err(refused())
		}
	}

	struct RefuseDictionary;

	impl SyncHooks for RefuseDictionary {
		fn on_intern(&self, _dictionary: DictionaryId) -> Outcome {
			Outcome::Err(refused())
		}

		fn on_resolve(&self, _dictionary: DictionaryId) -> Outcome {
			Outcome::Err(refused())
		}
	}

	struct AbsentLookups;

	impl SyncHooks for AbsentLookups {
		fn on_lookup(&self, _target: LookupTarget) -> LookupOutcome {
			LookupOutcome::Absent
		}
	}

	struct RefuseLookups;

	impl SyncHooks for RefuseLookups {
		fn on_lookup(&self, _target: LookupTarget) -> LookupOutcome {
			LookupOutcome::Err(refused())
		}
	}

	struct RecordCalls {
		seen: Mutex<Vec<u64>>,
	}

	impl SyncHooks for RecordCalls {
		fn on_call(&self, call: u64) -> Continue {
			self.seen.lock().push(call);
			Continue::Yes
		}
	}

	struct CrashAt(u64);

	impl SyncHooks for CrashAt {
		fn on_call(&self, call: u64) -> Continue {
			if call == self.0 {
				Continue::Crash
			} else {
				Continue::Yes
			}
		}
	}

	#[test]
	fn a_refused_write_returns_the_injected_error_and_leaves_the_rows_untouched() {
		let mut inner = MemoryTxn::default();
		inner.set(&key(1), row(1)).unwrap();
		let before = inner.rows.clone();
		let mut txn = TestingTxn::over(inner, Arc::new(RefuseWrites));

		assert_eq!(txn.set(&key(2), row(2)), Err(refused()));
		assert_eq!(txn.remove(&key(1)), Err(refused()));

		assert_eq!(txn.txn().rows, before);
	}

	#[test]
	fn a_refused_emit_returns_the_injected_error_and_records_no_entry_or_emission() {
		let mut txn = TestingTxn::over(MemoryTxn::default(), Arc::new(RefuseEmits));

		assert_eq!(txn.emit(VIEW, Diff::insert(Columns::empty())), Err(refused()));

		assert!(txn.txn().entries.is_empty());
		assert!(txn.txn().emitted.is_empty());
	}

	#[test]
	fn a_refused_intern_adds_no_value_and_a_refused_resolve_returns_the_injected_error() {
		let mut inner = MemoryTxn::default();
		let sol = inner.intern(&symbols(), &utf8("sol")).unwrap();
		let mut txn = TestingTxn::over(inner, Arc::new(RefuseDictionary));

		assert_eq!(txn.intern(&symbols(), &utf8("eth")), Err(refused()));
		assert_eq!(txn.resolve(&symbols(), sol), Err(refused()));

		assert_eq!(txn.txn().dictionary_values, BTreeMap::from([(SYMBOLS, vec![utf8("sol")])]));
	}

	#[test]
	fn an_absent_flows_lookup_hides_the_flows_the_inner_transaction_holds() {
		let mut txn = TestingTxn::over(catalog(), Arc::new(AbsentLookups));

		let flows = txn.transactional_flows().unwrap();

		assert!(flows.is_empty());
		assert_eq!(txn.txn().flows.len(), 1);
	}

	#[test]
	fn a_refused_lookup_returns_the_injected_error_for_every_target() {
		let mut txn = TestingTxn::over(MemoryTxn::default(), Arc::new(RefuseLookups));

		assert_eq!(txn.transactional_flows().unwrap_err(), refused());
		assert_eq!(txn.view(VIEW), Err(refused()));
		assert_eq!(txn.table(TABLE), Err(refused()));
		assert_eq!(txn.dictionary(SYMBOLS), Err(refused()));
	}

	#[test]
	#[should_panic(expected = "lookup of view 4 cannot be absent")]
	fn an_absent_view_lookup_is_an_invariant_breach_and_panics() {
		let mut txn = TestingTxn::over(catalog(), Arc::new(AbsentLookups));

		let _ = txn.view(VIEW);
	}

	#[test]
	fn only_set_remove_emit_and_intern_count_as_calls() {
		let hooks = Arc::new(RecordCalls {
			seen: Mutex::new(Vec::new()),
		});
		let mut txn = TestingTxn::over(catalog(), hooks.clone());

		txn.get(&key(1)).unwrap();
		txn.set(&key(1), row(1)).unwrap();
		txn.remove(&key(1)).unwrap();
		txn.emit(VIEW, Diff::insert(Columns::empty())).unwrap();
		let sol = txn.intern(&symbols(), &utf8("sol")).unwrap();
		txn.resolve(&symbols(), sol).unwrap();
		txn.transactional_flows().unwrap();
		txn.view(VIEW).unwrap();
		txn.table(TABLE).unwrap();
		txn.dictionary(SYMBOLS).unwrap();
		txn.cursor();
		txn.entries_from(0);
		txn.set_cursor(1);
		txn.now();

		assert_eq!(txn.calls(), 4);
		assert_eq!(*hooks.seen.lock(), vec![1, 2, 3, 4]);
	}

	#[test]
	fn a_crash_at_the_third_call_keeps_the_first_two_writes_and_stops_before_the_third() {
		let mut txn = TestingTxn::over(MemoryTxn::default(), Arc::new(CrashAt(3)));

		let payload = catch_unwind(AssertUnwindSafe(|| {
			txn.set(&key(1), row(1)).unwrap();
			txn.set(&key(2), row(2)).unwrap();
			txn.set(&key(3), row(3)).unwrap();
		}))
		.unwrap_err();

		assert_eq!(payload.downcast_ref::<String>().map(String::as_str), Some("simulated crash at call 3"));
		assert!(txn.txn().rows.contains_key(&key(1)));
		assert!(txn.txn().rows.contains_key(&key(2)));
		assert!(!txn.txn().rows.contains_key(&key(3)));
		assert_eq!(txn.calls(), 3);
	}
}
