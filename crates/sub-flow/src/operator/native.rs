// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	cell::{Cell, UnsafeCell},
	collections::HashMap,
	panic::{AssertUnwindSafe, catch_unwind},
	path::{Path, PathBuf},
	process::abort,
	sync::OnceLock,
};

use libloading::Symbol;
use reifydb_abi::operator::{capabilities::OperatorCapability, timer::TimerKind};
use reifydb_codec::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			flow::FlowNodeId,
			id::{NamespaceId, TableId},
			namespace::Namespace,
			table::Table,
		},
		change::Change,
	},
	key::operator_state::{GroupId, GroupSet, StateKey},
	metrics::heap::OperatorSample,
};
use reifydb_extension::loader::ffi::LibraryCache;
use reifydb_flow::{
	operator::Operator,
	transaction::{
		FlowTransaction,
		slot::{PersistFn, zero_usage},
		timer::Timer,
	},
};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_sdk::{
	config::Config,
	error::{Result as SdkResult, SdkError},
	operator::{OperatorLogic, timer::Timer as SdkTimer, view::native::NativeChangeView},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		constraint::TypeConstraint,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		row_number::RowNumber,
	},
};
use tracing::error;

use crate::{
	engine::{lease_demand, lease_report_from_sample},
	error::NativeOperatorError,
	operator::{
		BoxedOperator,
		context::native::{NativeBridge, NativeOperatorContext},
	},
};

fn run_or_abort<R>(node: FlowNodeId, stage: &'static str, f: impl FnOnce() -> SdkResult<R>) -> R {
	match catch_unwind(AssertUnwindSafe(f)) {
		Ok(Ok(value)) => value,
		Ok(Err(e)) => {
			error!(
				operator_id = node.0,
				stage, "native operator returned an error; operators must not fail - aborting: {:?}", e
			);
			abort();
		}
		Err(_) => {
			error!(operator_id = node.0, stage, "native operator panicked - aborting");
			abort();
		}
	}
}

pub const NATIVE_OPERATOR_MAGIC: u32 = 0x5244_424E;

pub const NATIVE_ABI_TAG: u32 = 0x030A;

pub type NativeOperatorCreateFn = fn(FlowNodeId, &Config) -> Result<BoxedBridgedOperator>;

pub struct NativeOperatorColumn {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

pub struct NativeOperatorDescriptor {
	pub abi_tag: u32,
	pub name: String,
	pub version: String,
	pub description: String,
	pub capabilities: u32,
	pub input_columns: Vec<NativeOperatorColumn>,
	pub output_columns: Vec<NativeOperatorColumn>,
}

pub fn native_operator_magic() -> u32 {
	NATIVE_OPERATOR_MAGIC
}

pub fn check_native_abi_tag(abi_tag: u32) -> Result<()> {
	if abi_tag != NATIVE_ABI_TAG {
		return Err(Error::from(NativeOperatorError::AbiTagMismatch {
			plugin: abi_tag,
			host: NATIVE_ABI_TAG,
		}));
	}
	Ok(())
}

pub trait BridgedOperator: Send {
	fn id(&self) -> FlowNodeId;

	fn capabilities(&self) -> &'static [OperatorCapability];

	fn apply(&self, bridge: &mut dyn NativeBridge, change: Change) -> Result<Change>;

	fn on_timer(&self, _bridge: &mut dyn NativeBridge, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn seal_after_ms(&self) -> Option<u64> {
		None
	}

	fn invalidate_groups(&self, _groups: &GroupSet) {}

	fn flush_state(&self, _bridge: &mut dyn NativeBridge) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

pub type BoxedBridgedOperator = Box<dyn BridgedOperator>;

pub struct FlowNativeBridge<'a> {
	txn: &'a mut FlowTransaction,
	node: FlowNodeId,
	now: DateTime,
}

impl<'a> FlowNativeBridge<'a> {
	pub fn new(txn: &'a mut FlowTransaction, node: FlowNodeId) -> Self {
		let now = txn.clock().now();
		Self {
			txn,
			node,
			now,
		}
	}
}

impl NativeBridge for FlowNativeBridge<'_> {
	fn clock_now(&self) -> DateTime {
		self.now
	}
	fn version(&self) -> CommitVersion {
		self.txn.version()
	}
	fn state_lease_bytes(&self) -> u64 {
		self.txn.state_budget()
			.current_lease(self.node)
			.map(|lease| lease.grant.bytes().as_bytes())
			.unwrap_or(0)
	}
	fn state_get(&mut self, key: &StateKey) -> Result<Option<EncodedRow>> {
		self.txn.state_get(self.node, key)
	}
	fn state_get_many(&mut self, keys: &[StateKey]) -> Result<Vec<(StateKey, EncodedRow)>> {
		Ok(self.txn
			.state_get_many(self.node, keys)?
			.items
			.into_iter()
			.filter_map(|r| StateKey::from_framed(r.key).map(|k| (k, r.row)))
			.collect())
	}
	fn state_set(&mut self, key: &StateKey, value: EncodedRow) -> Result<()> {
		self.txn.state_set(self.node, key, value)
	}
	fn state_remove(&mut self, key: &StateKey) -> Result<()> {
		self.txn.state_remove(self.node, key)
	}
	fn state_clear(&mut self) -> Result<()> {
		self.txn.state_clear(self.node)
	}
	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(StateKey, EncodedRow)>> {
		Ok(self.txn
			.state_range_all(self.node, range)?
			.items
			.into_iter()
			.filter_map(|r| StateKey::from_framed(r.key).map(|k| (k, r.row)))
			.collect())
	}
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<GroupId>> {
		Ok(self.txn.intern_groups(self.node, groups)?.into_iter().map(|(group, _)| group).collect())
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		groups.iter().map(|group| self.txn.lookup_group(self.node, group)).collect()
	}
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.arm_timer(
			self.node,
			&Timer {
				at,
				kind,
				key: key.clone(),
			},
		)
	}
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.node, group, keys)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.txn.remove_row_number(self.node, group, key).map(|_| ())
	}
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		self.txn.remove_row_numbers_below(self.node, group, upper)
	}
	fn store_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		self.txn.get(key)
	}
	fn store_contains(&mut self, key: &EncodedKey) -> Result<bool> {
		self.txn.contains_key(key)
	}
	fn store_prefix(&mut self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		Ok(self.txn.prefix(prefix)?.items.into_iter().map(|r| (r.key, r.row)).collect())
	}
	fn store_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		let rows = self.txn.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>()?;
		Ok(rows.into_iter().map(|r| (r.key, r.row)).collect())
	}
	fn catalog_find_namespace(
		&mut self,
		namespace: NamespaceId,
		version: CommitVersion,
	) -> Result<Option<Namespace>> {
		Ok(self.txn.host_catalog().find_namespace(namespace, version))
	}
	fn catalog_find_namespace_by_name(
		&mut self,
		namespace: &str,
		version: CommitVersion,
	) -> Result<Option<Namespace>> {
		Ok(self.txn.host_catalog().find_namespace_by_name(namespace, version))
	}
	fn catalog_find_table(&mut self, table: TableId, version: CommitVersion) -> Result<Option<Table>> {
		Ok(self.txn.host_catalog().find_table(table, version))
	}
	fn catalog_find_table_by_name(
		&mut self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Result<Option<Table>> {
		Ok(self.txn.host_catalog().find_table_by_name(namespace, name, version))
	}
	fn catalog_find_row_shape(&mut self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>> {
		Ok(self.txn.host_catalog().find_row_shape(fingerprint))
	}
	fn dictionary_id_by_name(&mut self, name: &str) -> Result<Option<DictionaryId>> {
		Ok(self.txn.find_dictionary_by_name(name).map(|d| d.id))
	}
	fn dictionary_find(&mut self, dictionary: DictionaryId, value: &Value) -> Result<Option<DictionaryEntryId>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.find_in_dictionary(&dict, value),
			None => Ok(None),
		}
	}
	fn dictionary_get(&mut self, dictionary: DictionaryId, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self.txn.find_dictionary(dictionary) {
			Some(dict) => self.txn.get_from_dictionary(&dict, id),
			None => Ok(None),
		}
	}
	fn state_get_many_visit(
		&mut self,
		keys: &[StateKey],
		visit: &mut dyn FnMut(&StateKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let batch = self.txn.state_get_many(self.node, keys).map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &batch.items {
			let Some(key) = StateKey::from_framed(r.key.clone()) else {
				continue;
			};
			visit(&key, &r.row)?;
		}
		Ok(())
	}
	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		visit: &mut dyn FnMut(&StateKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let batch = self.txn.state_range_all(self.node, range).map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &batch.items {
			let Some(key) = StateKey::from_framed(r.key.clone()) else {
				continue;
			};
			visit(&key, &r.row)?;
		}
		Ok(())
	}
	fn store_range_visit(
		&mut self,
		range: EncodedKeyRange,
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let rows =
			self.txn.range(range, RangeScope::All, 1024)
				.collect::<Result<Vec<_>>>()
				.map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &rows {
			visit(&r.key, &r.row)?;
		}
		Ok(())
	}
	fn store_prefix_visit(
		&mut self,
		prefix: &EncodedKey,
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let batch = self.txn.prefix(prefix).map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &batch.items {
			visit(&r.key, &r.row)?;
		}
		Ok(())
	}
}

pub struct LoadedNativeOperatorInfo {
	pub operator: String,
	pub library_path: PathBuf,
	pub version: String,
	pub description: String,
	pub input_columns: Vec<NativeOperatorColumn>,
	pub output_columns: Vec<NativeOperatorColumn>,
	pub capabilities: u32,
}

static GLOBAL_NATIVE_OPERATOR_LOADER: OnceLock<RwLock<NativeOperatorLoader>> = OnceLock::new();

pub fn native_operator_loader() -> &'static RwLock<NativeOperatorLoader> {
	GLOBAL_NATIVE_OPERATOR_LOADER.get_or_init(|| RwLock::new(NativeOperatorLoader::new()))
}

pub struct NativeOperatorLoader {
	cache: LibraryCache,
	operator_paths: HashMap<String, PathBuf>,
}

impl NativeOperatorLoader {
	fn new() -> Self {
		Self {
			cache: LibraryCache::new(),
			operator_paths: HashMap::new(),
		}
	}

	fn load_library(&mut self, path: &Path) -> Result<bool> {
		self.cache.check_magic(path, b"reifydb_native_operator_magic\0", NATIVE_OPERATOR_MAGIC).map_err(|_e| {
			Error::from(NativeOperatorError::LibraryNotLoaded {
				path: path.display().to_string(),
			})
		})
	}

	fn descriptor(&self, path: &Path) -> Result<NativeOperatorDescriptor> {
		let library = self.cache.get(path).ok_or_else(|| {
			Error::from(NativeOperatorError::LibraryNotLoaded {
				path: path.display().to_string(),
			})
		})?;

		let descriptor = unsafe {
			let get_descriptor: Symbol<fn() -> NativeOperatorDescriptor> =
				library.get(b"reifydb_native_operator_descriptor\0").map_err(|e| {
					Error::from(NativeOperatorError::SymbolNotFound {
						symbol: "reifydb_native_operator_descriptor",
						cause: e.to_string(),
					})
				})?;
			get_descriptor()
		};

		check_native_abi_tag(descriptor.abi_tag)?;

		Ok(descriptor)
	}

	pub fn register_operator(&mut self, path: &Path) -> Result<Option<LoadedNativeOperatorInfo>> {
		if !self.load_library(path)? {
			return Ok(None);
		}

		let descriptor = self.descriptor(path)?;
		self.operator_paths.insert(descriptor.name.clone(), path.to_path_buf());

		Ok(Some(LoadedNativeOperatorInfo {
			operator: descriptor.name,
			library_path: path.to_path_buf(),
			version: descriptor.version,
			description: descriptor.description,
			input_columns: descriptor.input_columns,
			output_columns: descriptor.output_columns,
			capabilities: descriptor.capabilities,
		}))
	}

	pub fn has_operator(&self, operator: &str) -> bool {
		self.operator_paths.contains_key(operator)
	}

	pub fn create_operator_by_name(
		&mut self,
		operator: &str,
		operator_id: FlowNodeId,
		config: &Config,
	) -> Result<BoxedOperator> {
		let path = self
			.operator_paths
			.get(operator)
			.ok_or_else(|| {
				Error::from(NativeOperatorError::OperatorNotFound {
					operator: operator.to_string(),
				})
			})?
			.clone();

		if !self.load_library(&path)? {
			return Err(Error::from(NativeOperatorError::LibraryNotLoaded {
				path: operator.to_string(),
			}));
		}

		self.descriptor(&path)?;

		let library = self.cache.get(&path).unwrap();
		let create: NativeOperatorCreateFn = unsafe {
			let create_symbol: Symbol<NativeOperatorCreateFn> =
				library.get(b"reifydb_native_operator_create\0").map_err(|e| {
					Error::from(NativeOperatorError::SymbolNotFound {
						symbol: "reifydb_native_operator_create",
						cause: e.to_string(),
					})
				})?;
			*create_symbol
		};

		let bridged = create(operator_id, config)?;
		let capabilities = bridged.capabilities();
		Ok(Box::new(NativeBridgedOperator::new(bridged, operator_id, capabilities)))
	}
}

impl Default for NativeOperatorLoader {
	fn default() -> Self {
		Self::new()
	}
}

pub struct NativeOperatorAdapter<C> {
	logic: UnsafeCell<C>,
	node: FlowNodeId,
	capabilities: &'static [OperatorCapability],
}

impl<C> NativeOperatorAdapter<C> {
	pub fn new(logic: C, node: FlowNodeId, capabilities: &'static [OperatorCapability]) -> Self {
		Self {
			logic: UnsafeCell::new(logic),
			node,
			capabilities,
		}
	}
}

unsafe impl<C: Send> Send for NativeOperatorAdapter<C> {}

impl<C: OperatorLogic + 'static> BridgedOperator for NativeOperatorAdapter<C> {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &'static [OperatorCapability] {
		self.capabilities
	}

	fn apply(&self, bridge: &mut dyn NativeBridge, change: Change) -> Result<Change> {
		let version = change.version;
		let changed_at = change.changed_at;
		let mut ctx = NativeOperatorContext::new(bridge, self.node);
		{
			let view = NativeChangeView::new(&change);
			let logic = unsafe { &mut *self.logic.get() };
			run_or_abort(self.node, "apply", || logic.apply(&mut ctx, view));
		}
		let diffs = ctx.take_diffs();
		Ok(Change::from_flow(self.node, version, diffs, changed_at))
	}

	fn sample(&self) -> Option<OperatorSample> {
		let logic = unsafe { &*self.logic.get() };
		logic.sample()
	}

	fn seal_after_ms(&self) -> Option<u64> {
		let logic = unsafe { &*self.logic.get() };
		logic.seal_after_ms()
	}

	fn on_timer(&self, bridge: &mut dyn NativeBridge, timer: Timer) -> Result<Option<Change>> {
		let at = timer.at;
		let version = bridge.version();
		let mut ctx = NativeOperatorContext::new(bridge, self.node);
		{
			let logic = unsafe { &mut *self.logic.get() };
			run_or_abort(self.node, "on_timer", || {
				logic.on_timer(
					&mut ctx,
					SdkTimer {
						at,
						kind: timer.kind,
						key: timer.key.as_ref(),
					},
				)
			});
		}
		let diffs = ctx.take_diffs();
		if diffs.is_empty() {
			return Ok(None);
		}
		Ok(Some(Change::from_flow(self.node, version, diffs, at)))
	}

	fn invalidate_groups(&self, groups: &GroupSet) {
		let logic = unsafe { &mut *self.logic.get() };
		logic.invalidate_groups(groups);
	}

	fn flush_state(&self, bridge: &mut dyn NativeBridge) -> Result<()> {
		let mut ctx = NativeOperatorContext::new(bridge, self.node);
		let logic = unsafe { &mut *self.logic.get() };
		run_or_abort(self.node, "flush_state", || logic.flush_state(&mut ctx));
		Ok(())
	}
}

#[derive(Clone, Copy)]
struct SendableBridged(*const dyn BridgedOperator);
unsafe impl Send for SendableBridged {}

pub struct NativeBridgedOperator {
	inner: BoxedBridgedOperator,
	node: FlowNodeId,
	capabilities: &'static [OperatorCapability],
	last_registered_txn: Cell<u64>,
}

impl NativeBridgedOperator {
	pub fn new(inner: BoxedBridgedOperator, node: FlowNodeId, capabilities: &'static [OperatorCapability]) -> Self {
		Self {
			inner,
			node,
			capabilities,
			last_registered_txn: Cell::new(u64::MAX),
		}
	}

	fn ensure_flush_slot(&self, txn: &mut FlowTransaction) -> Result<()> {
		let txn_version = txn.version().0;
		if self.last_registered_txn.get() != txn_version {
			let captured = SendableBridged(&*self.inner as *const dyn BridgedOperator);
			let node = self.node;
			let persist: PersistFn = Box::new(move |txn: &mut FlowTransaction, _value: Box<dyn Any>| {
				let captured = captured;
				let bridged = unsafe { &*captured.0 };
				let mut bridge = FlowNativeBridge::new(txn, node);
				bridged.flush_state(&mut bridge)?;
				let budget = txn.state_budget();
				match bridged.sample() {
					Some(sample) => {
						let report = lease_report_from_sample(&sample);
						budget.report_lease(node, report);
						budget.resize_lease_to_demand(node, lease_demand(&report));
					}
					None => budget.report_lease_none(node),
				}
				Ok(())
			});
			let _ = txn.operator_state::<(), _>(node, zero_usage, move |_txn| Ok(((), persist)))?;
			txn.mark_state_dirty(node);
			self.last_registered_txn.set(txn_version);
		}
		Ok(())
	}
}

unsafe impl Send for NativeBridgedOperator {}

impl Operator for NativeBridgedOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.capabilities
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.ensure_flush_slot(txn)?;
		let mut bridge = FlowNativeBridge::new(txn, self.node);
		self.inner.apply(&mut bridge, change)
	}

	fn seal_after_ms(&self) -> Option<u64> {
		self.inner.seal_after_ms()
	}

	fn invalidate_groups(&self, groups: &GroupSet) {
		self.inner.invalidate_groups(groups)
	}

	fn on_timer(&self, txn: &mut FlowTransaction, timer: Timer) -> Result<Option<Change>> {
		self.ensure_flush_slot(txn)?;
		let mut bridge = FlowNativeBridge::new(txn, self.node);
		self.inner.on_timer(&mut bridge, timer)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.inner.sample()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_abi::constants::OPERATOR_ABI_TAG;
	use reifydb_core::{
		common::CommitVersion,
		interface::change::Change,
		key::operator_state::{GroupId, GroupSet},
		state::horizon::{Horizon, Position},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_extension::operator::ffi_loader::check_operator_abi_tag;
	use reifydb_flow::{operator::Operator, transaction::ChangeCoordinate};
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::{
		Result,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::{
		BridgedOperator, EncodedKey, FlowNativeBridge, FlowNodeId, NATIVE_ABI_TAG, NativeBridge,
		NativeBridgedOperator, OperatorCapability, check_native_abi_tag,
	};

	const NODE: FlowNodeId = FlowNodeId(1);

	fn key(name: &str) -> EncodedKey {
		EncodedKey::new(name.as_bytes())
	}

	#[test]
	fn a_dylib_read_resolves_a_group_without_creating_one() {
		// Reads cross this bridge on eviction and diagnostic paths, which walk groups that may
		// already be reclaimed. If a read interned, every such walk would resurrect the groups it
		// visited and the dictionary could never shrink - the append lesson, now on the dylib seam
		// where the driver, not the host, decides which keys get touched.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(0),
			version: CommitVersion(7),
		});
		let mut bridge = FlowNativeBridge::new(&mut txn, NODE);

		assert_eq!(bridge.lookup_groups(&[key("absent")]).unwrap(), vec![None]);

		let interned = bridge.intern_groups(&[key("absent")]).unwrap();
		assert_eq!(
			interned,
			vec![GroupId::FIRST],
			"the earlier read must not have consumed an id from the counter"
		);
	}

	#[test]
	fn the_substrate_stamps_what_a_driver_can_no_longer_supply() {
		// A driver has no access to the transaction and, since positions were removed from the
		// intern surface, no way to pass one at all. The substrate stamps every intern from the
		// change coordinate set for the dispatch: an undeclared or version-domain node takes the
		// change version, an event-domain node takes the change's event time. A bridge that let
		// a driver value through - or stamped a clock reading - would run the bucket arithmetic
		// against a number from a different scale and the group would either never come due or
		// come due instantly, silently in release.
		// The coordinate's version deliberately differs from the transaction's own so the stamp
		// source is pinned: a batch spans several change versions inside one transaction, and the
		// stamp must follow the CHANGE being dispatched, not the transaction snapshot.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(42)).deferred();
		let at = DateTime::from_millis(1_700_000_000_123);
		txn.set_change_coordinate(ChangeCoordinate {
			at,
			version: CommitVersion(41),
		});

		let mut bridge = FlowNativeBridge::new(&mut txn, NODE);
		bridge.intern_groups(&[key("undeclared")]).unwrap();
		assert_eq!(
			txn.node_position(NODE).unwrap(),
			Position(at),
			"a node with no declared horizon stamps the same substrate coordinate as one that seals; \
			 there is no second domain for it to fall back to"
		);

		let sealed = FlowNodeId(8);
		txn.group_interner().set_horizon(sealed, Horizon::of(Duration::from_milliseconds(1_000).unwrap()));
		let mut bridge = FlowNativeBridge::new(&mut txn, sealed);
		bridge.intern_groups(&[key("sealed")]).unwrap();
		assert_eq!(txn.node_position(sealed).unwrap(), Position(at));
	}

	// A plugin whose abi_tag does not match the host's must be refused, so an
	// operator built against a different reifydb/toolchain is never loaded.
	#[test]
	fn native_abi_tag_accepts_match_rejects_mismatch() {
		assert!(check_native_abi_tag(NATIVE_ABI_TAG).is_ok());
		assert!(check_native_abi_tag(NATIVE_ABI_TAG ^ 0x1).is_err());
		assert!(check_native_abi_tag(0).is_err());
	}

	#[test]
	fn ffi_abi_tag_accepts_match_rejects_mismatch() {
		assert!(check_operator_abi_tag(OPERATOR_ABI_TAG).is_ok());
		assert!(check_operator_abi_tag(OPERATOR_ABI_TAG ^ 0x1).is_err());
		assert!(check_operator_abi_tag(0).is_err());
	}

	// The two tags must be distinct and must reject each other, so a native
	// `.so` can never validate against the ffi check or vice versa.
	#[test]
	fn native_and_ffi_tags_do_not_accept_each_other() {
		assert_ne!(NATIVE_ABI_TAG, OPERATOR_ABI_TAG);
		assert!(check_native_abi_tag(OPERATOR_ABI_TAG).is_err());
		assert!(check_operator_abi_tag(NATIVE_ABI_TAG).is_err());
	}

	struct RecordingBridged {
		invalidated: Arc<Mutex<Vec<GroupId>>>,
	}

	impl BridgedOperator for RecordingBridged {
		fn id(&self) -> FlowNodeId {
			NODE
		}

		fn capabilities(&self) -> &'static [OperatorCapability] {
			&[]
		}

		fn apply(&self, _bridge: &mut dyn NativeBridge, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn seal_after_ms(&self) -> Option<u64> {
			Some(65_000)
		}

		fn invalidate_groups(&self, groups: &GroupSet) {
			self.invalidated.lock().extend_from_slice(groups.as_slice());
		}
	}

	#[test]
	fn the_host_wrapper_forwards_seal_span_and_reclaimed_groups_to_the_dylib() {
		// NativeBridgedOperator is the host-side Operator over a dylib's BridgedOperator.
		// Its Operator impl forwarded seal_after_ms but silently dropped invalidate_groups
		// (trait default no-op), so the reclaim driver could erase a native operator's
		// group state on disk while the dylib kept serving it from RAM. Both must cross
		// this seam.
		let invalidated = Arc::new(Mutex::new(Vec::new()));
		let wrapper = NativeBridgedOperator::new(
			Box::new(RecordingBridged {
				invalidated: invalidated.clone(),
			}),
			NODE,
			&[],
		);

		assert_eq!(Operator::seal_after_ms(&wrapper), Some(65_000));

		Operator::invalidate_groups(&wrapper, &GroupSet::new([GroupId(3), GroupId(9)]));
		assert_eq!(*invalidated.lock(), vec![GroupId(3), GroupId(9)]);
	}
}
