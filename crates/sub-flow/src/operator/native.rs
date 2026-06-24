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
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
		shape::{RowShape, fingerprint::RowShapeFingerprint},
	},
	interface::{
		catalog::{
			flow::FlowNodeId,
			id::{NamespaceId, TableId},
			namespace::Namespace,
			table::Table,
		},
		change::Change,
	},
	internal,
};
use reifydb_extension::loader::ffi::LibraryCache;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_sdk::{
	config::Config,
	error::{Result as SdkResult, SdkError},
	operator::{OperatorLogic, Tick, view::native::NativeChangeView},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		constraint::TypeConstraint,
		dictionary::{DictionaryEntryId, DictionaryId},
		duration::Duration,
		row_number::RowNumber,
	},
};
use tracing::error;

use crate::{
	operator::{
		BoxedOperator, Operator,
		context::native::{NativeBridge, NativeOperatorContext},
		stateful::row::allocate_row_numbers,
	},
	transaction::{FlowTransaction, slot::PersistFn},
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

pub const NATIVE_ABI_TAG: u32 = 0x0308;

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
		return Err(Error(Box::new(internal!(
			"native operator ABI tag mismatch: plugin reports {:#06x}, host expects {:#06x}",
			abi_tag,
			NATIVE_ABI_TAG
		))));
	}
	Ok(())
}

pub trait BridgedOperator: Send {
	fn id(&self) -> FlowNodeId;

	fn capabilities(&self) -> &'static [OperatorCapability];

	fn apply(&self, bridge: &mut dyn NativeBridge, change: Change) -> Result<Change>;

	fn tick(&self, _bridge: &mut dyn NativeBridge, _tick: Tick) -> Result<Option<Change>> {
		Ok(None)
	}

	fn ticks(&self) -> Option<Duration> {
		None
	}

	fn flush_state(&self, _bridge: &mut dyn NativeBridge) -> Result<()> {
		Ok(())
	}
}

pub type BoxedBridgedOperator = Box<dyn BridgedOperator>;

pub struct FlowNativeBridge<'a> {
	txn: &'a mut FlowTransaction,
	node: FlowNodeId,
	now_nanos: u64,
}

impl<'a> FlowNativeBridge<'a> {
	pub fn new(txn: &'a mut FlowTransaction, node: FlowNodeId) -> Self {
		let now_nanos = txn.clock().now_nanos();
		Self {
			txn,
			node,
			now_nanos,
		}
	}
}

impl NativeBridge for FlowNativeBridge<'_> {
	fn clock_now_nanos(&self) -> u64 {
		self.now_nanos
	}
	fn state_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		self.txn.state_get(self.node, key)
	}
	fn state_get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		Ok(self.txn.state_get_many(self.node, keys)?.items.into_iter().map(|r| (r.key, r.row)).collect())
	}
	fn state_set(&mut self, key: &EncodedKey, value: EncodedRow) -> Result<()> {
		self.txn.state_set(self.node, key, value)
	}
	fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.state_remove(self.node, key)
	}
	fn state_drop(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.state_drop(self.node, key)
	}
	fn state_clear(&mut self) -> Result<()> {
		self.txn.state_clear(self.node)
	}
	fn state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		Ok(self.txn.state_range_all(self.node, range)?.items.into_iter().map(|r| (r.key, r.row)).collect())
	}
	fn internal_state_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		self.txn.internal_state_get(self.node, key)
	}
	fn internal_state_get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		Ok(self.txn
			.internal_state_get_many(self.node, keys)?
			.items
			.into_iter()
			.map(|r| (r.key, r.row))
			.collect())
	}
	fn internal_state_set(&mut self, key: &EncodedKey, value: EncodedRow) -> Result<()> {
		self.txn.internal_state_set(self.node, key, value)
	}
	fn internal_state_remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.internal_state_remove(self.node, key)
	}
	fn internal_state_drop(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.internal_state_drop(self.node, key)
	}
	fn internal_state_range(&mut self, range: EncodedKeyRange) -> Result<Vec<(EncodedKey, EncodedRow)>> {
		Ok(self.txn
			.internal_state_range_all(self.node, range)?
			.items
			.into_iter()
			.map(|r| (r.key, r.row))
			.collect())
	}
	fn allocate_row_numbers(&mut self, count: u64) -> Result<RowNumber> {
		allocate_row_numbers(self.txn, self.node, count).map(RowNumber)
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
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let batch = self.txn.state_get_many(self.node, keys).map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &batch.items {
			visit(&r.key, &r.row)?;
		}
		Ok(())
	}
	fn internal_state_get_many_visit(
		&mut self,
		keys: &[EncodedKey],
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let batch =
			self.txn.internal_state_get_many(self.node, keys)
				.map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &batch.items {
			visit(&r.key, &r.row)?;
		}
		Ok(())
	}
	fn state_range_visit(
		&mut self,
		range: EncodedKeyRange,
		visit: &mut dyn FnMut(&EncodedKey, &EncodedRow) -> SdkResult<()>,
	) -> SdkResult<()> {
		let batch = self.txn.state_range_all(self.node, range).map_err(|e| SdkError::Other(e.to_string()))?;
		for r in &batch.items {
			visit(&r.key, &r.row)?;
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
		self.cache
			.check_magic(path, b"reifydb_native_operator_magic\0", NATIVE_OPERATOR_MAGIC)
			.map_err(|e| Error(Box::new(internal!("{}", e))))
	}

	fn descriptor(&self, path: &Path) -> Result<NativeOperatorDescriptor> {
		let library = self
			.cache
			.get(path)
			.ok_or_else(|| Error(Box::new(internal!("Library not loaded: {}", path.display()))))?;

		let descriptor = unsafe {
			let get_descriptor: Symbol<fn() -> NativeOperatorDescriptor> =
				library.get(b"reifydb_native_operator_descriptor\0").map_err(|e| {
					Error(Box::new(internal!(
						"Failed to find reifydb_native_operator_descriptor: {}",
						e
					)))
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
			.ok_or_else(|| Error(Box::new(internal!("Native operator not found: {}", operator))))?
			.clone();

		if !self.load_library(&path)? {
			return Err(Error(Box::new(internal!(
				"Native operator library no longer valid: {}",
				operator
			))));
		}

		self.descriptor(&path)?;

		let library = self.cache.get(&path).unwrap();
		let create: NativeOperatorCreateFn = unsafe {
			let create_symbol: Symbol<NativeOperatorCreateFn> =
				library.get(b"reifydb_native_operator_create\0").map_err(|e| {
					Error(Box::new(internal!(
						"Failed to find reifydb_native_operator_create: {}",
						e
					)))
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

	fn ticks(&self) -> Option<Duration> {
		let logic = unsafe { &*self.logic.get() };
		logic.ticks()
	}

	fn tick(&self, bridge: &mut dyn NativeBridge, tick: Tick) -> Result<Option<Change>> {
		let now = tick.now;
		let mut ctx = NativeOperatorContext::new(bridge, self.node);
		{
			let logic = unsafe { &mut *self.logic.get() };
			run_or_abort(self.node, "tick", || logic.tick(&mut ctx, tick));
		}
		let diffs = ctx.take_diffs();
		if diffs.is_empty() {
			return Ok(None);
		}
		Ok(Some(Change::from_flow(self.node, CommitVersion(now.to_nanos()), diffs, now)))
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
				bridged.flush_state(&mut bridge)
			});
			let _ = txn.operator_state::<(), _>(node, move |_txn| Ok(((), persist)))?;
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

	fn ticks(&self) -> Option<Duration> {
		self.inner.ticks()
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		self.ensure_flush_slot(txn)?;
		let mut bridge = FlowNativeBridge::new(txn, self.node);
		self.inner.tick(&mut bridge, tick)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::constants::OPERATOR_ABI_TAG;
	use reifydb_extension::operator::ffi_loader::check_operator_abi_tag;

	use super::{NATIVE_ABI_TAG, check_native_abi_tag};

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
}
