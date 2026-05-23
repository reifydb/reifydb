// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	cell::{Cell, UnsafeCell},
	collections::{BTreeMap, HashMap},
	panic::{AssertUnwindSafe, catch_unwind},
	path::{Path, PathBuf},
	process::abort,
	sync::OnceLock,
	time::Duration,
};

use libloading::Symbol;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::FlowNodeId, change::Change},
	internal,
};
use reifydb_extension::loader::ffi::LibraryCache;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_sdk::{
	error::Result as SdkResult,
	operator::{OperatorLogic, Tick, view::native::NativeChangeView},
};
use reifydb_type::{
	Result,
	error::Error,
	value::{Value, constraint::TypeConstraint},
};
use tracing::error;

use crate::{
	operator::{BoxedOperator, Operator, context::native::NativeOperatorContext},
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

pub type NativeOperatorCreateFn = fn(FlowNodeId, &BTreeMap<String, Value>) -> Result<BoxedOperator>;

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
		config: &BTreeMap<String, Value>,
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

		create(operator_id, config)
	}
}

impl Default for NativeOperatorLoader {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Clone, Copy)]
struct SendableLogic<C>(*mut C);
unsafe impl<C: Send> Send for SendableLogic<C> {}

pub struct NativeOperatorAdapter<C> {
	logic: UnsafeCell<C>,
	node: FlowNodeId,
	capabilities: u32,
	last_registered_txn: Cell<u64>,
}

impl<C> NativeOperatorAdapter<C> {
	pub fn new(logic: C, node: FlowNodeId, capabilities: u32) -> Self {
		Self {
			logic: UnsafeCell::new(logic),
			node,
			capabilities,
			last_registered_txn: Cell::new(u64::MAX),
		}
	}
}

unsafe impl<C: Send> Send for NativeOperatorAdapter<C> {}

impl<C: OperatorLogic + 'static> NativeOperatorAdapter<C> {
	fn ensure_flush_slot(&self, txn: &mut FlowTransaction) -> Result<()> {
		let txn_version = txn.version().0;
		if self.last_registered_txn.get() != txn_version {
			let captured = SendableLogic(self.logic.get());
			let node = self.node;
			let persist: PersistFn = Box::new(move |txn: &mut FlowTransaction, _value: Box<dyn Any>| {
				let captured = captured;
				let logic = unsafe { &mut *captured.0 };
				let mut ctx = NativeOperatorContext::new(txn, node);
				run_or_abort(node, "flush_state", || logic.flush_state(&mut ctx));
				Ok(())
			});
			let _ = txn.operator_state::<(), _>(node, move |_txn| Ok(((), persist)))?;
			txn.mark_state_dirty(node);
			self.last_registered_txn.set(txn_version);
		}
		Ok(())
	}
}

impl<C: OperatorLogic + 'static> Operator for NativeOperatorAdapter<C> {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> u32 {
		self.capabilities
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.ensure_flush_slot(txn)?;
		let version = change.version;
		let changed_at = change.changed_at;
		let mut ctx = NativeOperatorContext::new(txn, self.node);
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

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		self.ensure_flush_slot(txn)?;
		let now = tick.now;
		let mut ctx = NativeOperatorContext::new(txn, self.node);
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
