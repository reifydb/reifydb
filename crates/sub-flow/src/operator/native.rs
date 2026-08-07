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
	interface::{catalog::flow::OperatorId, change::Change},
	key::operator_group_state::{GroupId, GroupStateKey},
	metrics::heap::OperatorSample,
};
use reifydb_extension::loader::ffi::LibraryCache;
use reifydb_flow::{
	operator::{BoxedOperator, Operator},
	timer::Timer,
	transaction::{
		FlowTransaction,
		slot::{PersistFn, zero_usage},
	},
};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_sdk::{
	config::Config,
	error::{Result as SdkResult, SdkError},
	operator::{OperatorLogic, timer::Timer as SdkTimer, view::native::NativeChangeView},
};
use reifydb_store_operator::floor::FloorSpec;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		constraint::TypeConstraint,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		duration::Duration,
		row_number::RowNumber,
	},
};
use tracing::error;

use crate::{
	engine::{lease_demand, lease_report_from_sample},
	error::NativeOperatorError,
	operator::{
		context::native::{NativeBridge, NativeOperatorContext},
		sealed_or_idle_floor,
	},
};

fn run_or_abort<R>(operator: OperatorId, stage: &'static str, f: impl FnOnce() -> SdkResult<R>) -> R {
	match catch_unwind(AssertUnwindSafe(f)) {
		Ok(Ok(value)) => value,
		Ok(Err(e)) => {
			error!(
				operator_id = operator.0,
				stage, "native operator returned an error; operators must not fail - aborting: {:?}", e
			);
			abort();
		}
		Err(_) => {
			error!(operator_id = operator.0, stage, "native operator panicked - aborting");
			abort();
		}
	}
}

pub const NATIVE_OPERATOR_MAGIC: u32 = 0x5244_424E;

pub const NATIVE_ABI_TAG: u32 = 0x030A;

pub type NativeOperatorCreateFn = fn(OperatorId, &Config) -> Result<BoxedBridgedOperator>;

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
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &'static [OperatorCapability];

	fn apply(&self, bridge: &mut dyn NativeBridge, change: Change) -> Result<Change>;

	fn on_timer(&self, _bridge: &mut dyn NativeBridge, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn seal_after(&self) -> Option<Duration> {
		None
	}

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
	operator: OperatorId,
	now: DateTime,
}

impl<'a> FlowNativeBridge<'a> {
	pub fn new(txn: &'a mut FlowTransaction, operator: OperatorId) -> Self {
		let now = txn.written_at();
		Self {
			txn,
			operator,
			now,
		}
	}
}

impl NativeBridge for FlowNativeBridge<'_> {
	fn written_at(&self) -> DateTime {
		self.now
	}
	fn version(&self) -> CommitVersion {
		self.txn.version()
	}
	fn state_lease_bytes(&self) -> u64 {
		self.txn.state_budget()
			.current_lease(self.operator)
			.map(|lease| lease.grant.bytes().as_bytes())
			.unwrap_or(0)
	}
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedRow>> {
		self.txn.state_get(self.operator, key)
	}
	fn state_get_many(&mut self, keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, EncodedRow)>> {
		Ok(self.txn
			.state_get_many(self.operator, keys)?
			.items
			.into_iter()
			.filter_map(|r| GroupStateKey::from_framed(r.key).map(|k| (k, r.row)))
			.collect())
	}
	fn state_set(&mut self, key: &GroupStateKey, value: EncodedRow) -> Result<()> {
		self.txn.state_set(self.operator, key, value)
	}
	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		self.txn.state_remove(self.operator, key)
	}
	fn state_clear(&mut self) -> Result<()> {
		self.txn.state_clear(self.operator)
	}
	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedRow)>> {
		Ok(self.txn
			.state_range_all(self.operator, range)?
			.items
			.into_iter()
			.filter_map(|r| GroupStateKey::from_framed(r.key).map(|k| (k, r.row)))
			.collect())
	}
	fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<GroupId>> {
		Ok(self.txn.intern_groups(self.operator, groups)?.into_iter().map(|(group, _)| group).collect())
	}
	fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		groups.iter().map(|group| self.txn.lookup_group(self.operator, group)).collect()
	}
	fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.arm_timer(
			self.operator,
			&Timer {
				at,
				kind,
				key: key.clone(),
			},
		)
	}
	fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		self.txn.disarm_timer(
			self.operator,
			&Timer {
				at,
				kind,
				key: key.clone(),
			},
		)
	}

	fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
		Ok(self.txn.flow_watermark())
	}
	fn get_or_create_row_numbers(&mut self, group: GroupId, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
		self.txn.get_or_create_row_numbers(self.operator, group, keys)
	}
	fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.txn.remove_row_number(self.operator, group, key).map(|_| ())
	}
	fn remove_row_numbers_below(&mut self, group: GroupId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		self.txn.remove_row_numbers_below(self.operator, group, upper)
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
	fn catalog_find_row_shape(&mut self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>> {
		Ok(self.txn.host_row_shape().find_row_shape(fingerprint))
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
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(&GroupStateKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let batch = self.txn.state_get_many(self.operator, keys).map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &batch.items {
			let Some(key) = GroupStateKey::from_framed(r.key.clone()) else {
				continue;
			};
			visit(&key, &r.row)?;
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

		// SAFETY: load_library accepted this path only after the native-operator magic symbol matched, so
		// the object was built against this crate and declares the descriptor symbol with this signature;
		// Symbol borrows library, which stays loaded for the call.
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
		operator_id: OperatorId,
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
		// SAFETY: load_library and descriptor accepted this path, so the object was built against this
		// crate and declares the create symbol with this signature; the copied-out pointer is called
		// before this method returns, while &mut self still holds the cache entry that keeps it mapped.
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
	operator: OperatorId,
	capabilities: &'static [OperatorCapability],
}

impl<C> NativeOperatorAdapter<C> {
	pub fn new(logic: C, operator: OperatorId, capabilities: &'static [OperatorCapability]) -> Self {
		Self {
			logic: UnsafeCell::new(logic),
			operator,
			capabilities,
		}
	}
}

unsafe impl<C: Send> Send for NativeOperatorAdapter<C> {}

impl<C: OperatorLogic + 'static> BridgedOperator for NativeOperatorAdapter<C> {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &'static [OperatorCapability] {
		self.capabilities
	}

	fn apply(&self, bridge: &mut dyn NativeBridge, change: Change) -> Result<Change> {
		let version = change.version;
		let changed_at = change.changed_at;
		let mut ctx = NativeOperatorContext::new(bridge, self.operator);
		{
			let view = NativeChangeView::new(&change);
			// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time, and the
			// logic only reaches the context, never back into this cell; no other borrow is live.
			let logic = unsafe { &mut *self.logic.get() };
			run_or_abort(self.operator, "apply", || logic.apply(&mut ctx, view));
		}
		let diffs = ctx.take_diffs();
		Ok(Change::from_flow(self.operator, version, diffs, changed_at))
	}

	fn sample(&self) -> Option<OperatorSample> {
		// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time and no apply or
		// timer call is in flight here; no other borrow of the cell is live.
		let logic = unsafe { &*self.logic.get() };
		logic.sample()
	}

	fn seal_after(&self) -> Option<Duration> {
		// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time and no apply or
		// timer call is in flight here; no other borrow of the cell is live.
		let logic = unsafe { &*self.logic.get() };
		logic.seal_after()
	}

	fn on_timer(&self, bridge: &mut dyn NativeBridge, timer: Timer) -> Result<Option<Change>> {
		let at = timer.at;
		let version = bridge.version();
		let mut ctx = NativeOperatorContext::new(bridge, self.operator);
		{
			// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time, and the
			// logic only reaches the context, never back into this cell; no other borrow is live.
			let logic = unsafe { &mut *self.logic.get() };
			run_or_abort(self.operator, "on_timer", || {
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
		Ok(Some(Change::from_flow(self.operator, version, diffs, at)))
	}

	fn flush_state(&self, bridge: &mut dyn NativeBridge) -> Result<()> {
		let mut ctx = NativeOperatorContext::new(bridge, self.operator);
		// SAFETY: the adapter is Send but not Sync, so one actor holds &self at a time, and the logic
		// only reaches the context, never back into this cell; no other borrow is live.
		let logic = unsafe { &mut *self.logic.get() };
		run_or_abort(self.operator, "flush_state", || logic.flush_state(&mut ctx));
		Ok(())
	}
}

#[derive(Clone, Copy)]
struct SendableBridged(*const dyn BridgedOperator);
unsafe impl Send for SendableBridged {}

pub struct NativeBridgedOperator {
	inner: BoxedBridgedOperator,
	operator: OperatorId,
	capabilities: &'static [OperatorCapability],
	last_registered_txn: Cell<u64>,
}

impl NativeBridgedOperator {
	pub fn new(
		inner: BoxedBridgedOperator,
		operator: OperatorId,
		capabilities: &'static [OperatorCapability],
	) -> Self {
		Self {
			inner,
			operator,
			capabilities,
			last_registered_txn: Cell::new(u64::MAX),
		}
	}

	fn ensure_flush_slot(&self, txn: &mut FlowTransaction) -> Result<()> {
		let txn_version = txn.version().0;
		if self.last_registered_txn.get() != txn_version {
			let captured = SendableBridged(&*self.inner as *const dyn BridgedOperator);
			let operator = self.operator;
			let persist: PersistFn = Box::new(move |txn: &mut FlowTransaction, _value: Box<dyn Any>| {
				let captured = captured;
				// SAFETY: captured.0 points at the heap allocation of self.inner, which is stable
				// across moves of the wrapper and outlives the transaction running this persist
				// closure, since the actor owning the operator also drives that transaction.
				let bridged = unsafe { &*captured.0 };
				let mut bridge = FlowNativeBridge::new(txn, operator);
				bridged.flush_state(&mut bridge)?;
				let budget = txn.state_budget();
				match bridged.sample() {
					Some(sample) => {
						let report = lease_report_from_sample(&sample);
						budget.report_lease(operator, report);
						budget.resize_lease_to_demand(operator, lease_demand(&report));
					}
					None => budget.report_lease_none(operator),
				}
				Ok(())
			});
			let _ = txn.operator_state::<(), _>(operator, zero_usage, move |_txn| Ok(((), persist)))?;
			txn.mark_state_dirty(operator);
			self.last_registered_txn.set(txn_version);
		}
		Ok(())
	}
}

unsafe impl Send for NativeBridgedOperator {}

impl Operator for NativeBridgedOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.capabilities
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.ensure_flush_slot(txn)?;
		let mut bridge = FlowNativeBridge::new(txn, self.operator);
		self.inner.apply(&mut bridge, change)
	}

	fn retention_scale(&self) -> Option<Duration> {
		self.inner.seal_after().filter(|span| !span.is_zero())
	}

	fn seal_span(&self) -> Option<Duration> {
		self.inner.seal_after().filter(|span| !span.is_zero())
	}

	fn floors(&self, txn: &mut FlowTransaction, watermark: DateTime) -> Result<FloorSpec> {
		sealed_or_idle_floor(txn, self.operator, watermark, self.retention_scale())
	}

	fn on_timer(&self, txn: &mut FlowTransaction, timer: Timer) -> Result<Option<Change>> {
		self.ensure_flush_slot(txn)?;
		let mut bridge = FlowNativeBridge::new(txn, self.operator);
		self.inner.on_timer(&mut bridge, timer)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.inner.sample()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::constants::OPERATOR_ABI_TAG;
	use reifydb_core::{common::CommitVersion, interface::change::Change, key::operator_group_state::GroupId};
	use reifydb_extension::operator::ffi_loader::check_operator_abi_tag;
	use reifydb_flow::{operator::Operator, transaction::ChangeCoordinate};
	use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
	use reifydb_value::{
		Result,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::{
		BridgedOperator, EncodedKey, FlowNativeBridge, NATIVE_ABI_TAG, NativeBridge, NativeBridgedOperator,
		OperatorCapability, OperatorId, check_native_abi_tag,
	};

	const NODE: OperatorId = OperatorId(1);

	fn key(name: &str) -> EncodedKey {
		EncodedKey::new(name.as_bytes())
	}

	#[test]
	fn a_dylib_read_resolves_a_group_without_creating_one() {
		// Eviction and diagnostic paths walk groups that may already be reclaimed, so a read that
		// interned would resurrect them and the dictionary could never shrink - and on this seam
		// it is the driver, not the host, that decides which keys get touched.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
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
	fn native_abi_tag_accepts_match_rejects_mismatch() {
		// A mismatched tag must be refused, or an operator built against a different toolchain
		// gets loaded.
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

	#[test]
	fn native_and_ffi_tags_do_not_accept_each_other() {
		// The two tags must reject each other, or a native `.so` validates against the ffi check.
		assert_ne!(NATIVE_ABI_TAG, OPERATOR_ABI_TAG);
		assert!(check_native_abi_tag(OPERATOR_ABI_TAG).is_err());
		assert!(check_operator_abi_tag(NATIVE_ABI_TAG).is_err());
	}

	struct RecordingBridged;

	impl BridgedOperator for RecordingBridged {
		fn id(&self) -> OperatorId {
			NODE
		}

		fn capabilities(&self) -> &'static [OperatorCapability] {
			&[]
		}

		fn apply(&self, _bridge: &mut dyn NativeBridge, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn seal_after(&self) -> Option<Duration> {
			Some(Duration::from_milliseconds_const(65_000))
		}
	}

	#[test]
	fn the_host_wrapper_forwards_the_seal_span_from_the_dylib() {
		// The retention scale sizes both the activity grid and the floor derivation, so a wrapper
		// that swallowed seal_after_ms would register a sealing native operator as perpetual.
		let wrapper = NativeBridgedOperator::new(Box::new(RecordingBridged), NODE, &[]);

		assert_eq!(Operator::retention_scale(&wrapper), Some(Duration::from_milliseconds(65_000).unwrap()));
	}

	#[test]
	fn the_host_wrapper_forwards_the_seal_span_to_the_frontier_walk() {
		// The walk subtracts this span, so a wrapper that swallowed it would claim a frontier covering buckets
		// the operator can still amend.
		let wrapper = NativeBridgedOperator::new(Box::new(RecordingBridged), NODE, &[]);

		assert_eq!(Operator::seal_span(&wrapper), Some(Duration::from_milliseconds(65_000).unwrap()));
	}
}
