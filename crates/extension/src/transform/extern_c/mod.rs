// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod loader;

use std::{cell::UnsafeCell, ffi::c_void, ptr};

use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::{
	common::extern_c::wire::callbacks::{builder::BuilderCallbacks, memory::MemoryCallbacks},
	error::SdkError,
	flow::operator::extern_c::binding::arena::Arena,
	transform::extern_c::wire::{
		callbacks::TransformCallbacks, context::ExternCContextRaw, descriptor::ExternCTransformDescriptor,
		vtable::ExternCTransformVTable,
	},
};
use reifydb_value::{self, Result};
use tracing::instrument;

use super::{Transform, context::TransformContext};
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
	procedure::callbacks::extern_c::memory,
};

thread_local! {
	static EXTERN_C_TRANSFORM_ARENA: UnsafeCell<Arena> = UnsafeCell::new(Arena::new());
}

pub struct ExternCTransform {
	#[allow(dead_code)]
	descriptor: ExternCTransformDescriptor,
	vtable: ExternCTransformVTable,
	instance: *mut c_void,

	builder_registry: BuilderRegistry,

	cached_ctx: UnsafeCell<ExternCContextRaw>,
}

impl ExternCTransform {
	pub fn new(descriptor: ExternCTransformDescriptor, instance: *mut c_void) -> Self {
		let vtable = descriptor.vtable;

		Self {
			descriptor,
			vtable,
			instance,
			builder_registry: BuilderRegistry::new(),
			cached_ctx: UnsafeCell::new(ExternCContextRaw {
				txn_ptr: ptr::null_mut(),
				executor_ptr: ptr::null(),
				written_at_nanos: 0,
				callbacks: pure_host_callbacks(),
			}),
		}
	}

	#[allow(dead_code)]
	pub(crate) fn descriptor(&self) -> &ExternCTransformDescriptor {
		&self.descriptor
	}
}

// SAFETY: `instance` and `cached_ctx` are unsynchronised; sound only because the flow engine never runs
// `apply` for one node concurrently.
unsafe impl Send for ExternCTransform {}
unsafe impl Sync for ExternCTransform {}

impl Drop for ExternCTransform {
	fn drop(&mut self) {
		if !self.instance.is_null() {
			// SAFETY: instance came from this descriptor's create; Drop runs at most once.
			unsafe { (self.vtable.destroy)(self.instance) };
		}
	}
}

impl Transform for ExternCTransform {
	#[instrument(name = "transform::extern_c::apply", level = "trace", skip_all)]
	fn apply(&self, ctx: &TransformContext, input: Columns) -> Result<Columns> {
		// SAFETY: the arena is thread-local and nothing marshalled into it outlives a call.
		EXTERN_C_TRANSFORM_ARENA.with(|cell| unsafe { (*cell.get()).clear() });
		let extern_c_input =
			EXTERN_C_TRANSFORM_ARENA.with(|cell| unsafe { (*cell.get()).marshal_columns(&input) });

		let extern_c_ctx_ptr = self.cached_ctx.get();
		// SAFETY: cached_ctx owns the ExternCContextRaw for the life of self and apply is not re-entrant.
		unsafe {
			(*extern_c_ctx_ptr).written_at_nanos = ctx.runtime_context.clock.now().to_nanos();
		}

		let result_code = with_registry(&self.builder_registry, || {
			// SAFETY: instance, ctx and the arena-held input all stay valid across the call.
			call_with_abort_on_panic("transform::apply", || unsafe {
				(self.vtable.transform)(self.instance, extern_c_ctx_ptr, &extern_c_input)
			})
		});

		if result_code != 0 {
			let _ = self.builder_registry.drain();
			return Err(SdkError::Other(format!(
				"extern-C transform apply failed with code: {}",
				result_code
			))
			.into());
		}

		Ok(single_columns_from_registry(&self.builder_registry))
	}
}

fn pure_host_callbacks() -> TransformCallbacks {
	TransformCallbacks {
		memory: MemoryCallbacks {
			alloc: memory::host_alloc,
			free: memory::host_free,
		},
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
