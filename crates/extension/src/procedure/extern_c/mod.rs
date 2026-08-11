// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod loader;

use std::{cell::UnsafeCell, ffi::c_void, ptr};

use reifydb_abi::{
	callbacks::{builder::BuilderCallbacks, host::HostCallbacks, memory::MemoryCallbacks, rql::RqlCallbacks},
	context::context::ExternCContext,
	procedure::{descriptor::ExternCProcedureDescriptor, vtable::ExternCProcedureVTable},
};
use reifydb_codec::value::encode_params;
use reifydb_core::value::column::columns::Columns;
use reifydb_routine_abi::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sdk::{error::SdkError, extern_c::arena::Arena};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{reifydb_assertions, value::value_type::ValueType};
use tracing::instrument;

use super::callbacks::extern_c::{memory, rql};
use crate::{
	callbacks::extern_c::{
		builder::{
			BuilderRegistry, host_builder_acquire, host_builder_bitvec_ptr, host_builder_commit,
			host_builder_data_ptr, host_builder_emit_diff, host_builder_grow, host_builder_offsets_ptr,
			host_builder_release, with_registry,
		},
		panic::call_with_abort_on_panic,
		single_columns_from_registry,
	},
	transform::extern_c::stubs,
};

thread_local! {
	static EXTERN_C_PROC_ARENA: UnsafeCell<Arena> = UnsafeCell::new(Arena::new());
}

pub struct ExternCProcedure {
	info: RoutineInfo,
	#[allow(dead_code)]
	descriptor: ExternCProcedureDescriptor,
	vtable: ExternCProcedureVTable,

	instance: Mutex<*mut c_void>,

	builder_registry: BuilderRegistry,

	cached_ctx: UnsafeCell<ExternCContext>,
}

impl ExternCProcedure {
	pub fn new(name: impl Into<String>, descriptor: ExternCProcedureDescriptor, instance: *mut c_void) -> Self {
		let vtable = descriptor.vtable;
		let name = name.into();

		Self {
			info: RoutineInfo::new(&name),
			descriptor,
			vtable,
			instance: Mutex::new(instance),
			builder_registry: BuilderRegistry::new(),
			cached_ctx: UnsafeCell::new(ExternCContext {
				txn_ptr: ptr::null_mut(),
				executor_ptr: ptr::null(),
				operator_id: 0,
				written_at_nanos: 0,
				callbacks: procedure_host_callbacks(),
			}),
		}
	}
}

// SAFETY: instance and cached_ctx are only touched while the instance Mutex is held.
unsafe impl Send for ExternCProcedure {}
unsafe impl Sync for ExternCProcedure {}

impl Drop for ExternCProcedure {
	fn drop(&mut self) {
		let instance = *self.instance.lock();
		if !instance.is_null() {
			// SAFETY: instance came from this descriptor's create; Drop runs at most once.
			unsafe { (self.vtable.destroy)(instance) };
		}
	}
}

fn procedure_host_callbacks() -> HostCallbacks {
	HostCallbacks {
		memory: MemoryCallbacks {
			alloc: memory::host_alloc,
			free: memory::host_free,
		},
		state: stubs::state(),
		rql: RqlCallbacks {
			rql: rql::host_rql,
		},
		dictionary: stubs::dictionary(),
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

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for ExternCProcedure {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	#[instrument(name = "procedure::extern_c::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let instance_guard = self.instance.lock();
		let instance = *instance_guard;

		let params_bytes = self.serialize_params(ctx)?;
		self.reset_arena();
		self.prepare_extern_c_context(ctx);
		let result_code = self.invoke_instance(instance, &params_bytes);

		let result = self.collect_or_drain(result_code);
		drop(instance_guard);
		result
	}
}

impl ExternCProcedure {
	#[inline]
	fn serialize_params(&self, ctx: &ProcedureContext<'_, '_>) -> Result<Vec<u8>, RoutineError> {
		encode_params(ctx.params).map_err(|e| {
			RoutineError::Wrapped(Box::new(
				SdkError::Other(format!("Failed to serialize params: {}", e)).into(),
			))
		})
	}

	#[inline]
	fn reset_arena(&self) {
		// SAFETY: the arena is thread-local and nothing marshalled into it outlives a call.
		EXTERN_C_PROC_ARENA.with(|cell| unsafe { (*cell.get()).clear() });
	}

	#[inline]
	fn prepare_extern_c_context(&self, ctx: &mut ProcedureContext<'_, '_>) {
		let extern_c_ctx_ptr = self.cached_ctx.get();
		// SAFETY: cached_ctx owns the ExternCContext for the life of self; execute holds the Mutex.
		unsafe {
			(*extern_c_ctx_ptr).txn_ptr = ctx.tx as *mut Transaction<'_> as *mut c_void;
			(*extern_c_ctx_ptr).written_at_nanos = ctx.runtime_context.clock.now().to_nanos();
		}
	}

	#[inline]
	fn invoke_instance(&self, instance: *mut c_void, params_bytes: &[u8]) -> i32 {
		reifydb_assertions! {
			assert!(
				!instance.is_null(),
				"extern-C procedure instance pointer is null at call time; invoking vtable.call with a \
				 null instance is undefined behaviour inside the dlopen'd operator and would \
				 crash the host"
			);
		}

		let extern_c_ctx_ptr = self.cached_ctx.get();
		with_registry(&self.builder_registry, || {
			// SAFETY: instance is non-null under the held Mutex; params_bytes outlives the call.
			call_with_abort_on_panic("procedure::call", || unsafe {
				(self.vtable.call)(
					instance,
					extern_c_ctx_ptr,
					params_bytes.as_ptr(),
					params_bytes.len(),
				)
			})
		})
	}

	#[inline]
	fn collect_or_drain(&self, result_code: i32) -> Result<Columns, RoutineError> {
		if result_code != 0 {
			let _ = self.builder_registry.drain();
			return Err(RoutineError::Wrapped(Box::new(
				SdkError::Other(format!("extern-C procedure call failed with code: {}", result_code))
					.into(),
			)));
		}

		Ok(single_columns_from_registry(&self.builder_registry))
	}
}
