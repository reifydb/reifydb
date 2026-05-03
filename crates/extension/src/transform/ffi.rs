// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{cell::UnsafeCell, ffi::c_void, ptr};

use reifydb_abi::{
	callbacks::{builder::BuilderCallbacks, host::HostCallbacks, log::LogCallbacks, memory::MemoryCallbacks},
	context::context::ContextFFI,
	transform::{descriptor::TransformDescriptorFFI, vtable::TransformVTableFFI},
};
use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::{error::FFIError, ffi::arena::Arena};
use reifydb_type::{self, Result};
use tracing::instrument;

use super::{Transform, context::TransformContext};
use crate::{
	ffi_callbacks::{
		builder::{
			BuilderRegistry, host_builder_acquire, host_builder_bitvec_ptr, host_builder_commit,
			host_builder_data_ptr, host_builder_emit_diff, host_builder_grow, host_builder_offsets_ptr,
			host_builder_release, with_registry,
		},
		panic::call_with_abort_on_panic,
		single_columns_from_registry,
	},
	procedure::ffi_callbacks::{logging, memory},
};

thread_local! {
	static FFI_TRANSFORM_ARENA: UnsafeCell<Arena> = UnsafeCell::new(Arena::new());
}

pub struct NativeTransformFFI {
	#[allow(dead_code)]
	descriptor: TransformDescriptorFFI,
	vtable: TransformVTableFFI,
	instance: *mut c_void,

	builder_registry: BuilderRegistry,

	cached_ctx: UnsafeCell<ContextFFI>,
}

impl NativeTransformFFI {
	pub fn new(descriptor: TransformDescriptorFFI, instance: *mut c_void) -> Self {
		let vtable = descriptor.vtable;

		Self {
			descriptor,
			vtable,
			instance,
			builder_registry: BuilderRegistry::new(),
			cached_ctx: UnsafeCell::new(ContextFFI {
				txn_ptr: ptr::null_mut(),
				executor_ptr: ptr::null(),
				operator_id: 0,
				clock_now_nanos: 0,
				callbacks: pure_host_callbacks(),
			}),
		}
	}

	#[allow(dead_code)]
	pub(crate) fn descriptor(&self) -> &TransformDescriptorFFI {
		&self.descriptor
	}
}

// SAFETY: NativeTransformFFI is only accessed from a single context at a time.

unsafe impl Send for NativeTransformFFI {}
unsafe impl Sync for NativeTransformFFI {}

impl Drop for NativeTransformFFI {
	fn drop(&mut self) {
		if !self.instance.is_null() {
			unsafe { (self.vtable.destroy)(self.instance) };
		}
	}
}

impl Transform for NativeTransformFFI {
	#[instrument(name = "transform::ffi::apply", level = "debug", skip_all)]
	fn apply(&self, ctx: &TransformContext, input: Columns) -> Result<Columns> {
		// SAFETY: single-threaded per call; no live pointers from a prior

		FFI_TRANSFORM_ARENA.with(|cell| unsafe { (*cell.get()).clear() });
		let ffi_input = FFI_TRANSFORM_ARENA.with(|cell| unsafe { (*cell.get()).marshal_columns(&input) });

		let ffi_ctx_ptr = self.cached_ctx.get();
		unsafe {
			(*ffi_ctx_ptr).clock_now_nanos = ctx.runtime_context.clock.now_nanos();
		}

		let result_code = with_registry(&self.builder_registry, || {
			call_with_abort_on_panic("transform::apply", || unsafe {
				(self.vtable.transform)(self.instance, ffi_ctx_ptr, &ffi_input)
			})
		});

		if result_code != 0 {
			let _ = self.builder_registry.drain();
			return Err(FFIError::Other(format!("FFI transform apply failed with code: {}", result_code))
				.into());
		}

		Ok(single_columns_from_registry(&self.builder_registry))
	}
}

fn pure_host_callbacks() -> HostCallbacks {
	HostCallbacks {
		memory: MemoryCallbacks {
			alloc: memory::host_alloc,
			free: memory::host_free,
			realloc: memory::host_realloc,
		},
		state: stubs::state(),
		log: LogCallbacks {
			message: logging::host_log_message,
		},
		store: stubs::store(),
		catalog: stubs::catalog(),
		rql: stubs::rql(),
		builder: BuilderCallbacks {
			acquire: host_builder_acquire,
			data_ptr: host_builder_data_ptr,
			offsets_ptr: host_builder_offsets_ptr,
			bitvec_ptr: host_builder_bitvec_ptr,
			grow: host_builder_grow,
			commit: host_builder_commit,
			release: host_builder_release,
			emit_diff: host_builder_emit_diff,
		},
	}
}

pub(crate) mod stubs {
	use reifydb_abi::{
		callbacks::{
			catalog::CatalogCallbacks, rql::RqlCallbacks, state::StateCallbacks, store::StoreCallbacks,
		},
		catalog::{namespace::NamespaceFFI, table::TableFFI},
		constants::FFI_ERROR_INTERNAL,
		context::{
			context::ContextFFI,
			iterators::{StateIteratorFFI, StoreIteratorFFI},
		},
		data::buffer::BufferFFI,
	};

	pub fn state() -> StateCallbacks {
		StateCallbacks {
			get: state_get,
			set: state_set,
			remove: state_remove,
			clear: state_clear,
			prefix: state_prefix,
			range: state_range,
			iterator_next: state_iterator_next,
			iterator_free: state_iterator_free,
		}
	}

	extern "C" fn state_get(_: u64, _: *mut ContextFFI, _: *const u8, _: usize, _: *mut BufferFFI) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn state_set(_: u64, _: *mut ContextFFI, _: *const u8, _: usize, _: *const u8, _: usize) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn state_remove(_: u64, _: *mut ContextFFI, _: *const u8, _: usize) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn state_clear(_: u64, _: *mut ContextFFI) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn state_prefix(
		_: u64,
		_: *mut ContextFFI,
		_: *const u8,
		_: usize,
		_: *mut *mut StateIteratorFFI,
	) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn state_range(
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
	extern "C" fn state_iterator_next(_: *mut StateIteratorFFI, _: *mut BufferFFI, _: *mut BufferFFI) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn state_iterator_free(_: *mut StateIteratorFFI) {}

	pub fn store() -> StoreCallbacks {
		StoreCallbacks {
			get: store_get,
			contains_key: store_contains_key,
			prefix: store_prefix,
			range: store_range,
			iterator_next: store_iterator_next,
			iterator_free: store_iterator_free,
		}
	}

	extern "C" fn store_get(_: *mut ContextFFI, _: *const u8, _: usize, _: *mut BufferFFI) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn store_contains_key(_: *mut ContextFFI, _: *const u8, _: usize, _: *mut u8) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn store_prefix(_: *mut ContextFFI, _: *const u8, _: usize, _: *mut *mut StoreIteratorFFI) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn store_range(
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
	extern "C" fn store_iterator_next(_: *mut StoreIteratorFFI, _: *mut BufferFFI, _: *mut BufferFFI) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn store_iterator_free(_: *mut StoreIteratorFFI) {}

	pub fn catalog() -> CatalogCallbacks {
		CatalogCallbacks {
			find_namespace: catalog_find_namespace,
			find_namespace_by_name: catalog_find_namespace_by_name,
			find_table: catalog_find_table,
			find_table_by_name: catalog_find_table_by_name,
			free_namespace: catalog_free_namespace,
			free_table: catalog_free_table,
		}
	}

	extern "C" fn catalog_find_namespace(_: *mut ContextFFI, _: u64, _: u64, _: *mut NamespaceFFI) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn catalog_find_namespace_by_name(
		_: *mut ContextFFI,
		_: *const u8,
		_: usize,
		_: u64,
		_: *mut NamespaceFFI,
	) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn catalog_find_table(_: *mut ContextFFI, _: u64, _: u64, _: *mut TableFFI) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn catalog_find_table_by_name(
		_: *mut ContextFFI,
		_: u64,
		_: *const u8,
		_: usize,
		_: u64,
		_: *mut TableFFI,
	) -> i32 {
		FFI_ERROR_INTERNAL
	}
	extern "C" fn catalog_free_namespace(_: *mut NamespaceFFI) {}
	extern "C" fn catalog_free_table(_: *mut TableFFI) {}

	pub fn rql() -> RqlCallbacks {
		RqlCallbacks {
			rql: rql_unsupported,
		}
	}

	unsafe extern "C" fn rql_unsupported(
		_: *mut ContextFFI,
		_: *const u8,
		_: usize,
		_: *const u8,
		_: usize,
		_: *mut BufferFFI,
	) -> i32 {
		FFI_ERROR_INTERNAL
	}
}
