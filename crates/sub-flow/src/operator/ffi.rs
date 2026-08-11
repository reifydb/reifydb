// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	cell::{Cell, UnsafeCell},
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
	ptr,
};

use reifydb_abi::{
	callbacks::builder::EmitDiffKind,
	constants::{FFI_OK, FFI_SAMPLE_NO_DATA},
	context::context::ContextFFI,
	data::state::StateUsageFFI,
	flow::change::ChangeFFI,
	operator::{
		capabilities::{OperatorCapability, from_bitmask},
		descriptor::OperatorDescriptorFFI,
		vtable::OperatorVTableFFI,
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff, Diffs},
	},
	metrics::heap::{OperatorSample, StateMemory},
	value::column::columns::Columns,
};
use reifydb_engine::vm::executor::Executor;
use reifydb_extension::callbacks::builder::{BuilderRegistry, with_registry};
use reifydb_flow::{
	operator::{Operator, scale_from_millis},
	timer::Timer,
	transaction::{DepFlowTransaction, slot::PersistFn},
};
use reifydb_sdk::{error::SdkError, ffi::arena::Arena};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::{Span, error, field, instrument};

use crate::ffi::{callbacks::create_host_callbacks, context::new_ffi_context};

thread_local! {
	static FFI_MARSHAL_ARENA: UnsafeCell<Arena> = UnsafeCell::new(Arena::new());
}

#[derive(Clone, Copy)]
struct SendableInstance(*mut c_void);
unsafe impl Send for SendableInstance {}
unsafe impl Sync for SendableInstance {}

pub struct FFIOperatorHandle {
	capabilities: Box<[OperatorCapability]>,

	vtable: OperatorVTableFFI,

	instance: *mut c_void,

	operator_id: OperatorId,

	executor: Executor,

	builder_registry: BuilderRegistry,

	last_registered_txn: Cell<u64>,

	cached_ctx: UnsafeCell<ContextFFI>,
}

impl FFIOperatorHandle {
	pub fn new(
		descriptor: OperatorDescriptorFFI,
		instance: *mut c_void,
		operator_id: OperatorId,
		executor: Executor,
	) -> Self {
		let vtable = descriptor.vtable;
		let capabilities = from_bitmask(descriptor.capabilities).into_boxed_slice();

		Self {
			capabilities,
			vtable,
			instance,
			operator_id,
			executor,
			builder_registry: BuilderRegistry::new(),
			last_registered_txn: Cell::new(u64::MAX),
			cached_ctx: UnsafeCell::new(ContextFFI {
				txn_ptr: ptr::null_mut(),
				executor_ptr: ptr::null(),
				operator_id: operator_id.0,
				written_at_nanos: 0,
				callbacks: create_host_callbacks(),
			}),
		}
	}

	fn ensure_txn_setup(&self, txn: &mut DepFlowTransaction) -> Result<()> {
		let txn_version = txn.version().0;
		// SAFETY: one actor drives this operator and no guest call is in flight here, so
		// the context cell is not aliased while this &mut exists.
		let ctx = unsafe { &mut *self.cached_ctx.get() };
		if self.last_registered_txn.get() != txn_version {
			ensure_flush_slot(txn, self.operator_id, self.vtable, self.instance, self.executor.clone())?;
			self.last_registered_txn.set(txn_version);
			ctx.txn_ptr = txn as *mut _ as *mut c_void;
			ctx.executor_ptr = &self.executor as *const _ as *const c_void;
		}
		ctx.written_at_nanos = txn.written_at().to_nanos();
		Ok(())
	}
}

// SAFETY: FFIOperatorHandle is only accessed from a single actor at a time.
unsafe impl Send for FFIOperatorHandle {}

impl Drop for FFIOperatorHandle {
	fn drop(&mut self) {
		if !self.instance.is_null() {
			unsafe { (self.vtable.destroy)(self.instance) };
		}
	}
}

#[inline]
#[instrument(name = "flow::ffi::marshal", level = "trace", skip_all)]
fn marshal_input(arena: &mut Arena, change: &Change) -> ChangeFFI {
	arena.marshal_change(change)
}

#[inline]
#[instrument(name = "flow::ffi::vtable_call", level = "trace", skip_all, fields(operator_id = operator_id.0))]
fn call_vtable(
	vtable: &OperatorVTableFFI,
	instance: *mut c_void,
	ffi_ctx_ptr: *mut ContextFFI,
	ffi_input: &ChangeFFI,
	operator_id: OperatorId,
) -> i32 {
	// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
	// FFIOperatorHandle::drop calls destroy; ffi_ctx_ptr and ffi_input point at caller-owned values that outlive
	// the call, and the host holds no Rust borrow of either while the guest runs.
	let result = catch_unwind(AssertUnwindSafe(|| unsafe { (vtable.apply)(instance, ffi_ctx_ptr, ffi_input) }));

	match result {
		Ok(code) => code,
		Err(panic_info) => {
			let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
				s.to_string()
			} else if let Some(s) = panic_info.downcast_ref::<String>() {
				s.clone()
			} else {
				"Unknown panic".to_string()
			};
			error!(operator_id = operator_id.0, "FFI operator panicked during apply: {}", msg);
			abort();
		}
	}
}

fn ensure_flush_slot(
	txn: &mut DepFlowTransaction,
	operator_id: OperatorId,
	vtable: OperatorVTableFFI,
	instance: *mut c_void,
	executor: Executor,
) -> Result<()> {
	let send_instance = SendableInstance(instance);
	let _ = txn.operator_state(operator_id, move |_txn| {
		let captured_instance = send_instance;
		let captured_vtable = vtable;
		let captured_executor = executor;
		let captured_id = operator_id;
		let persist: PersistFn = Box::new(move |txn, _value: Box<dyn Any>| {
			let ffi_ctx = new_ffi_context(txn, &captured_executor, captured_id, create_host_callbacks());
			let ffi_ctx_ptr = &ffi_ctx as *const _ as *mut ContextFFI;
			let inst = captured_instance;
			let mut usage = StateUsageFFI::default();
			// SAFETY: captured_vtable and inst.0 come from the loaded operator's descriptor and the
			// operator outlives the transaction running this persist closure; ffi_ctx and usage are
			// locals that stay alive for the call with no Rust borrow of them live during it.
			let result = catch_unwind(AssertUnwindSafe(|| unsafe {
				(captured_vtable.flush_state)(inst.0, ffi_ctx_ptr, &mut usage)
			}));
			match result {
				Ok(FFI_OK) | Ok(FFI_SAMPLE_NO_DATA) => Ok(()),
				Ok(code) => Err(SdkError::Other(format!(
					"FFI operator flush_state failed with code: {}",
					code
				))
				.into()),
				Err(_) => {
					error!(operator_id = captured_id.0, "FFI operator panicked during flush_state");
					abort();
				}
			}
		});

		Ok(((), persist))
	})?;
	txn.mark_state_dirty(operator_id);
	Ok(())
}

impl Operator for FFIOperatorHandle {
	fn id(&self) -> OperatorId {
		self.operator_id
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		&self.capabilities
	}

	fn seal_span(&self) -> Option<Duration> {
		// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
		// Drop calls destroy; the call passes no host pointers.
		scale_from_millis(Some(unsafe { (self.vtable.seal_after_ms)(self.instance) }))
	}

	#[instrument(name = "flow::ffi::apply", level = "trace", skip_all, fields(
		operator_id = self.operator_id.0,
		input_diff_count = change.diffs.len(),
		output_diff_count = field::Empty
	))]
	fn apply(&self, txn: &mut DepFlowTransaction, change: Change) -> Result<Change> {
		self.ensure_txn_setup(txn)?;

		// SAFETY: the arena is thread-local and the previous apply's guest call has returned, so
		// no pointer into it is still live when it is cleared and re-borrowed.
		FFI_MARSHAL_ARENA.with(|cell| unsafe { (*cell.get()).clear() });
		let ffi_input = FFI_MARSHAL_ARENA.with(|cell| marshal_input(unsafe { &mut *cell.get() }, &change));

		let version = change.version;
		let changed_at = change.changed_at;

		let ffi_ctx_ptr = self.cached_ctx.get();

		let result_code = with_registry(&self.builder_registry, || {
			call_vtable(&self.vtable, self.instance, ffi_ctx_ptr, &ffi_input, self.operator_id)
		});

		if result_code != 0 {
			let _ = self.builder_registry.drain();
			return Err(
				SdkError::Other(format!("FFI operator apply failed with code: {}", result_code)).into()
			);
		}

		let output_change = drain_emitted_diffs(&self.builder_registry, self.operator_id, version, changed_at);

		Span::current().record("output_diff_count", output_change.diffs.len());

		Ok(output_change)
	}

	#[instrument(name = "flow::ffi::on_timer", level = "trace", skip_all, fields(
		operator_id = self.operator_id.0,
		output_diff_count = field::Empty
	))]
	fn on_timer(&self, txn: &mut DepFlowTransaction, timer: Timer) -> Result<Option<Change>> {
		self.ensure_txn_setup(txn)?;

		let version = txn.version();
		let key = timer.key.as_ref();
		let ffi_ctx_ptr = self.cached_ctx.get();

		// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
		// Drop calls destroy; ffi_ctx_ptr is this operator's cached ContextFFI with no Rust borrow of it
		// live during the call, and key's ptr/len describe a slice of `timer`, which outlives the call.
		let result_code = self.invoke_under_panic_guard("on_timer", || unsafe {
			(self.vtable.on_timer)(
				self.instance,
				ffi_ctx_ptr,
				timer.at.to_millis(),
				timer.kind as u8,
				key.as_ptr(),
				key.len(),
			)
		});

		if result_code < 0 {
			let _ = self.builder_registry.drain();
			return Err(SdkError::Other(format!(
				"FFI operator on_timer failed with code: {}",
				result_code
			))
			.into());
		}

		let output_change = drain_emitted_diffs(&self.builder_registry, self.operator_id, version, timer.at);
		Span::current().record("output_diff_count", output_change.diffs.len());
		if output_change.diffs.is_empty() {
			return Ok(None);
		}
		Ok(Some(output_change))
	}

	fn sample(&self) -> Option<OperatorSample> {
		let mut usage = StateUsageFFI::default();
		// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
		// Drop calls destroy; `usage` is an initialised local, exclusively borrowed for the call.
		match unsafe { (self.vtable.sample)(self.instance, &mut usage) } {
			FFI_OK => Some(sample_from_usage(&usage)),
			FFI_SAMPLE_NO_DATA => None,
			code => {
				error!(
					operator_id = self.operator_id.0,
					code, "FFI operator failed to report state usage"
				);
				None
			}
		}
	}
}

fn sample_from_usage(usage: &StateUsageFFI) -> OperatorSample {
	let state = StateMemory::new(Count::new(usage.state_entries), ByteSize::from_bytes(usage.state_bytes));
	let row_numbers =
		StateMemory::new(Count::new(usage.row_number_entries), ByteSize::from_bytes(usage.row_number_bytes));
	OperatorSample::with_memory(state).with_row_number_cache(row_numbers)
}

impl FFIOperatorHandle {
	#[inline]
	fn invoke_under_panic_guard<F>(&self, op: &'static str, call: F) -> i32
	where
		F: FnOnce() -> i32,
	{
		with_registry(&self.builder_registry, || {
			let result = catch_unwind(AssertUnwindSafe(call));
			match result {
				Ok(code) => code,
				Err(panic_info) => {
					let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
						s.to_string()
					} else if let Some(s) = panic_info.downcast_ref::<String>() {
						s.clone()
					} else {
						"Unknown panic".to_string()
					};
					error!(
						operator_id = self.operator_id.0,
						"FFI operator panicked during {}: {}", op, msg
					);
					abort();
				}
			}
		})
	}
}

fn drain_emitted_diffs(
	registry: &BuilderRegistry,
	operator_id: OperatorId,
	version: CommitVersion,
	changed_at: DateTime,
) -> Change {
	let emitted = registry.drain();
	let diffs: Diffs = emitted
		.into_iter()
		.map(|d| match d.kind {
			EmitDiffKind::Insert => Diff::insert(d.post.unwrap_or_else(Columns::empty)),
			EmitDiffKind::Update => Diff::update(
				d.pre.unwrap_or_else(Columns::empty),
				d.post.unwrap_or_else(Columns::empty),
			),
			EmitDiffKind::Remove => Diff::remove(d.pre.unwrap_or_else(Columns::empty)),
		})
		.collect();
	Change::from_flow(operator_id, version, diffs, changed_at)
}

#[cfg(test)]
mod tests {
	use reifydb_abi::data::state::StateUsageFFI;

	use super::sample_from_usage;

	#[test]
	fn host_sample_decode_keeps_the_row_number_cache_off_the_state_total() {
		// The two pairs cross on separate fields; folding either into the other double-counts it.
		let usage = StateUsageFFI {
			state_entries: 3,
			state_bytes: 128,
			row_number_entries: 2,
			row_number_bytes: 64,
		};

		let sample = sample_from_usage(&usage);

		let memory = sample.memory.expect("state memory always ships");
		assert_eq!(memory.entries.as_u64(), 3);
		assert_eq!(memory.bytes.as_bytes(), 128);
		let rows = sample.row_number_cache.expect("row number cache always ships");
		assert_eq!(rows.entries.as_u64(), 2);
		assert_eq!(rows.bytes.as_bytes(), 64);
	}
}
