// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! FFI procedure implementation that bridges native shared-library procedures with ReifyDB

use std::{
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
	ptr,
	sync::Mutex,
};

use reifydb_abi::{
	callbacks::{
		catalog::CatalogCallbacks, host::HostCallbacks, log::LogCallbacks, memory::MemoryCallbacks,
		rql::RqlCallbacks, state::StateCallbacks, store::StoreCallbacks,
	},
	constants::FFI_ERROR_INTERNAL,
	context::context::ContextFFI,
	data::{buffer::BufferFFI, column::ColumnsFFI},
	procedure::{descriptor::ProcedureDescriptorFFI, vtable::ProcedureVTableFFI},
};
use reifydb_core::value::column::columns::Columns;
use reifydb_routine::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_sdk::ffi::arena::Arena;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::r#type::Type;
use tracing::{error, instrument};

use super::ffi_callbacks::{logging, memory, rql};

/// FFI procedure that wraps an external procedure implementation
pub struct NativeProcedureFFI {
	info: RoutineInfo,
	/// Procedure descriptor from the FFI library
	#[allow(dead_code)]
	descriptor: ProcedureDescriptorFFI,
	/// Virtual function table for calling FFI functions
	vtable: ProcedureVTableFFI,
	/// Pointer to the FFI procedure instance.
	/// Wrapped in a Mutex because the FFI vtable's `call` is not declared
	/// thread-safe at the ABI level  - the host serialises invocations.
	instance: Mutex<*mut c_void>,
}

impl NativeProcedureFFI {
	/// Create a new FFI procedure
	pub fn new(name: impl Into<String>, descriptor: ProcedureDescriptorFFI, instance: *mut c_void) -> Self {
		let vtable = descriptor.vtable;
		let name = name.into();

		Self {
			info: RoutineInfo::new(&name),
			descriptor,
			vtable,
			instance: Mutex::new(instance),
		}
	}
}

// SAFETY: NativeProcedureFFI is only accessed from a single context at a time.
unsafe impl Send for NativeProcedureFFI {}
unsafe impl Sync for NativeProcedureFFI {}

impl Drop for NativeProcedureFFI {
	fn drop(&mut self) {
		let instance = *self.instance.get_mut().unwrap();
		if !instance.is_null() {
			unsafe { (self.vtable.destroy)(instance) };
		}
	}
}

/// Create host callbacks for FFI procedures.
///
/// Uses real memory/logging/rql callbacks, and stubs for state/store/catalog
/// (which are not relevant for procedure execution).
fn create_procedure_host_callbacks() -> HostCallbacks {
	HostCallbacks {
		memory: MemoryCallbacks {
			alloc: memory::host_alloc,
			free: memory::host_free,
			realloc: memory::host_realloc,
		},
		state: stub_state_callbacks(),
		log: LogCallbacks {
			message: logging::host_log_message,
		},
		store: stub_store_callbacks(),
		catalog: stub_catalog_callbacks(),
		rql: RqlCallbacks {
			rql: rql::host_rql,
		},
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for NativeProcedureFFI {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	#[instrument(name = "procedure::ffi::execute", level = "debug", skip_all)]
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let mut arena = Arena::new();

		// Set thread-local arena for host_alloc
		memory::set_current_arena(&mut arena as *mut Arena);
		let instance_guard = self.instance.lock().unwrap();
		let instance = *instance_guard;

		// Serialize params to postcard bytes
		let params_bytes = to_stdvec(ctx.params).map_err(|e| {
			RoutineError::Wrapped(Box::new(
				FFIError::Other(format!("Failed to serialize params: {}", e)).into(),
			))
		})?;

		// Build ContextFFI with real callbacks
		let callbacks = create_procedure_host_callbacks();
		let mut ctx_ffi = ContextFFI {
			txn_ptr: ctx.tx as *mut Transaction<'_> as *mut c_void,
			executor_ptr: ptr::null(),
			operator_id: 0,
			callbacks,
		};

		let mut ffi_output = ColumnsFFI::empty();

		let result = catch_unwind(AssertUnwindSafe(|| unsafe {
			(self.vtable.call)(
				instance,
				&mut ctx_ffi,
				params_bytes.as_ptr(),
				params_bytes.len(),
				&mut ffi_output,
			)
		}));

		let result_code = match result {
			Ok(code) => code,
			Err(panic_info) => {
				let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
					s.to_string()
				} else if let Some(s) = panic_info.downcast_ref::<String>() {
					s.clone()
				} else {
					"Unknown panic".to_string()
				};
				error!("FFI procedure panicked during call: {}", msg);
				abort();
			}
		};

		if result_code != 0 {
			memory::clear_current_arena();
			drop(instance_guard);
			return Err(RoutineError::Wrapped(Box::new(
				FFIError::Other(format!("FFI procedure call failed with code: {}", result_code)).into(),
			)));
		}

		let columns = arena.unmarshal_columns(&ffi_output);

		memory::clear_current_arena();
		drop(instance_guard);

		Ok(columns)
	}
}

use postcard::to_stdvec;
use reifydb_abi::{
	catalog::{namespace::NamespaceFFI, table::TableFFI},
	context::iterators::{StateIteratorFFI, StoreIteratorFFI},
};
use reifydb_sdk::error::FFIError;

fn stub_state_callbacks() -> StateCallbacks {
	StateCallbacks {
		get: stub_state_get,
		set: stub_state_set,
		remove: stub_state_remove,
		clear: stub_state_clear,
		prefix: stub_state_prefix,
		range: stub_state_range,
		iterator_next: stub_state_iterator_next,
		iterator_free: stub_state_iterator_free,
	}
}

extern "C" fn stub_state_get(_: u64, _: *mut ContextFFI, _: *const u8, _: usize, _: *mut BufferFFI) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_state_set(_: u64, _: *mut ContextFFI, _: *const u8, _: usize, _: *const u8, _: usize) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_state_remove(_: u64, _: *mut ContextFFI, _: *const u8, _: usize) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_state_clear(_: u64, _: *mut ContextFFI) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_state_prefix(
	_: u64,
	_: *mut ContextFFI,
	_: *const u8,
	_: usize,
	_: *mut *mut StateIteratorFFI,
) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_state_range(
	_: u64,
	_: *mut ContextFFI,
	_: *const u8,
	_: usize,
	_: u8,
	_: *const u8,
	_: usize,
	_: u8,
	_: *mut *mut StateIteratorFFI,
) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_state_iterator_next(_: *mut StateIteratorFFI, _: *mut BufferFFI, _: *mut BufferFFI) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_state_iterator_free(_: *mut StateIteratorFFI) {}

fn stub_store_callbacks() -> StoreCallbacks {
	StoreCallbacks {
		get: stub_store_get,
		contains_key: stub_store_contains_key,
		prefix: stub_store_prefix,
		range: stub_store_range,
		iterator_next: stub_store_iterator_next,
		iterator_free: stub_store_iterator_free,
	}
}

extern "C" fn stub_store_get(_: *mut ContextFFI, _: *const u8, _: usize, _: *mut BufferFFI) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_store_contains_key(_: *mut ContextFFI, _: *const u8, _: usize, _: *mut u8) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_store_prefix(_: *mut ContextFFI, _: *const u8, _: usize, _: *mut *mut StoreIteratorFFI) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_store_range(
	_: *mut ContextFFI,
	_: *const u8,
	_: usize,
	_: u8,
	_: *const u8,
	_: usize,
	_: u8,
	_: *mut *mut StoreIteratorFFI,
) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_store_iterator_next(_: *mut StoreIteratorFFI, _: *mut BufferFFI, _: *mut BufferFFI) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_store_iterator_free(_: *mut StoreIteratorFFI) {}

fn stub_catalog_callbacks() -> CatalogCallbacks {
	CatalogCallbacks {
		find_namespace: stub_catalog_find_namespace,
		find_namespace_by_name: stub_catalog_find_namespace_by_name,
		find_table: stub_catalog_find_table,
		find_table_by_name: stub_catalog_find_table_by_name,
		free_namespace: stub_catalog_free_namespace,
		free_table: stub_catalog_free_table,
	}
}

extern "C" fn stub_catalog_find_namespace(_: *mut ContextFFI, _: u64, _: u64, _: *mut NamespaceFFI) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_catalog_find_namespace_by_name(
	_: *mut ContextFFI,
	_: *const u8,
	_: usize,
	_: u64,
	_: *mut NamespaceFFI,
) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_catalog_find_table(_: *mut ContextFFI, _: u64, _: u64, _: *mut TableFFI) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_catalog_find_table_by_name(
	_: *mut ContextFFI,
	_: u64,
	_: *const u8,
	_: usize,
	_: u64,
	_: *mut TableFFI,
) -> i32 {
	FFI_ERROR_INTERNAL
}
extern "C" fn stub_catalog_free_namespace(_: *mut NamespaceFFI) {}
extern "C" fn stub_catalog_free_table(_: *mut TableFFI) {}
