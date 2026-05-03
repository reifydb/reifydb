// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
	context::context::ContextFFI,
	flow::change::ChangeFFI,
	operator::{descriptor::OperatorDescriptorFFI, vtable::OperatorVTableFFI},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff, Diffs},
	},
	value::column::columns::Columns,
};
use reifydb_engine::vm::executor::Executor;
use reifydb_extension::ffi_callbacks::builder::{BuilderRegistry, with_registry};
use reifydb_sdk::{error::FFIError, ffi::arena::Arena, operator::Tick};
use reifydb_type::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};
use tracing::{Span, error, field, instrument};

use crate::{
	ffi::{callbacks::create_host_callbacks, context::new_ffi_context},
	operator::Operator,
	transaction::{FlowTransaction, slot::PersistFn},
};

thread_local! {
	static FFI_MARSHAL_ARENA: UnsafeCell<Arena> = UnsafeCell::new(Arena::new());
}

#[derive(Clone, Copy)]
struct SendableInstance(*mut c_void);
unsafe impl Send for SendableInstance {}
unsafe impl Sync for SendableInstance {}

pub struct FFIOperator {
	descriptor: OperatorDescriptorFFI,

	vtable: OperatorVTableFFI,

	instance: *mut c_void,

	operator_id: FlowNodeId,

	executor: Executor,

	builder_registry: BuilderRegistry,

	last_registered_txn: Cell<u64>,

	cached_ctx: UnsafeCell<ContextFFI>,
}

impl FFIOperator {
	pub fn new(
		descriptor: OperatorDescriptorFFI,
		instance: *mut c_void,
		operator_id: FlowNodeId,
		executor: Executor,
	) -> Self {
		let vtable = descriptor.vtable;

		Self {
			descriptor,
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
				clock_now_nanos: 0,
				callbacks: create_host_callbacks(),
			}),
		}
	}

	pub(crate) fn descriptor(&self) -> &OperatorDescriptorFFI {
		&self.descriptor
	}

	fn ensure_txn_setup(&self, txn: &mut FlowTransaction) -> Result<()> {
		let txn_version = txn.version().0;
		if self.last_registered_txn.get() != txn_version {
			ensure_flush_slot(txn, self.operator_id, self.vtable, self.instance, self.executor.clone())?;
			self.last_registered_txn.set(txn_version);
			// SAFETY: single-threaded actor; no aliasing with guest (vtable not

			let ctx = unsafe { &mut *self.cached_ctx.get() };
			ctx.txn_ptr = txn as *mut _ as *mut c_void;
			ctx.executor_ptr = &self.executor as *const _ as *const c_void;
			ctx.clock_now_nanos = txn.clock().now_nanos();
		}
		Ok(())
	}
}

// SAFETY: FFIOperator is only accessed from a single actor at a time.

unsafe impl Send for FFIOperator {}
unsafe impl Sync for FFIOperator {}

impl Drop for FFIOperator {
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
	operator_id: FlowNodeId,
) -> i32 {
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
	txn: &mut FlowTransaction,
	operator_id: FlowNodeId,
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
			let result = catch_unwind(AssertUnwindSafe(|| unsafe {
				(captured_vtable.flush_state)(inst.0, ffi_ctx_ptr)
			}));
			match result {
				Ok(0) => Ok(()),
				Ok(code) => Err(FFIError::Other(format!(
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

impl Operator for FFIOperator {
	fn id(&self) -> FlowNodeId {
		self.operator_id
	}

	#[instrument(name = "flow::ffi::apply", level = "debug", skip_all, fields(
		operator_id = self.operator_id.0,
		input_diff_count = change.diffs.len(),
		output_diff_count = field::Empty
	))]
	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.ensure_txn_setup(txn)?;

		// SAFETY: single-threaded per operator; no live pointers from a prior

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
				FFIError::Other(format!("FFI operator apply failed with code: {}", result_code)).into()
			);
		}

		let output_change = drain_emitted_diffs(&self.builder_registry, self.operator_id, version, changed_at);

		Span::current().record("output_diff_count", output_change.diffs.len());

		Ok(output_change)
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		self.ensure_txn_setup(txn)?;

		let row_numbers: Vec<u64> = rows.iter().map(|r| (*r).into()).collect();
		let ffi_ctx_ptr = self.cached_ctx.get();

		let result_code = self.invoke_under_panic_guard("pull", || unsafe {
			(self.vtable.pull)(self.instance, ffi_ctx_ptr, row_numbers.as_ptr(), row_numbers.len())
		});

		if result_code != 0 {
			let _ = self.builder_registry.drain();
			return Err(
				FFIError::Other(format!("FFI operator pull failed with code: {}", result_code)).into()
			);
		}

		let mut diffs = self.builder_registry.drain();
		let columns = if let Some(first) = diffs.drain(..).next() {
			first.post.or(first.pre).unwrap_or_else(Columns::empty)
		} else {
			Columns::empty()
		};

		Ok(columns)
	}

	#[instrument(name = "flow::ffi::tick", level = "debug", skip_all, fields(
		operator_id = self.operator_id.0,
		output_diff_count = field::Empty
	))]
	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		self.ensure_txn_setup(txn)?;

		let timestamp_nanos = tick.now.to_nanos();
		let ffi_ctx_ptr = self.cached_ctx.get();

		let result_code = self.invoke_under_panic_guard("tick", || unsafe {
			(self.vtable.tick)(self.instance, ffi_ctx_ptr, timestamp_nanos)
		});

		if result_code < 0 {
			let _ = self.builder_registry.drain();
			return Err(
				FFIError::Other(format!("FFI operator tick failed with code: {}", result_code)).into()
			);
		}

		if result_code == 1 {
			let _ = self.builder_registry.drain();
			return Ok(None);
		}

		let version = CommitVersion(timestamp_nanos);
		let output_change = drain_emitted_diffs(&self.builder_registry, self.operator_id, version, tick.now);
		Span::current().record("output_diff_count", output_change.diffs.len());
		Ok(Some(output_change))
	}
}

impl FFIOperator {
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
	operator_id: FlowNodeId,
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
