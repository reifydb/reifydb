// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cell::UnsafeCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff, Diffs},
		flow::{OperatorCapability, from_bitmask},
	},
	metrics::heap::{OperatorSample, StateMemory},
	value::column::columns::Columns,
};
use reifydb_extension::{
	callbacks::extern_c::builder::{BuilderRegistry, with_registry},
	operator::callbacks::extern_c::{
		context::{ExternCHostContext, new_extern_c_context},
		create_host_callbacks,
	},
};
use reifydb_flow::{
	operator::{HostOperator, host::HostContext, scale_from_millis},
	timer::Timer,
};
use reifydb_sdk::{
	common::extern_c::wire::{
		callbacks::builder::EmitDiffKind,
		status::{EXTERN_C_OK, EXTERN_C_SAMPLE_NO_DATA},
	},
	error::SdkError,
	flow::{
		extern_c::wire::change::ExternCChange,
		operator::extern_c::{
			binding::arena::Arena,
			wire::{
				context::ExternCContextRaw, descriptor::ExternCOperatorDescriptor,
				state::ExternCStateUsage, vtable::ExternCOperatorVTable,
			},
		},
	},
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::{Span, error, field, instrument};

thread_local! {
	static EXTERN_C_MARSHAL_ARENA: UnsafeCell<Arena> = UnsafeCell::new(Arena::new());
}

pub struct ExternCOperatorHandle {
	capabilities: Box<[OperatorCapability]>,

	vtable: ExternCOperatorVTable,

	instance: *mut c_void,

	operator_id: OperatorId,

	builder_registry: BuilderRegistry,
}

impl ExternCOperatorHandle {
	pub fn new(descriptor: ExternCOperatorDescriptor, instance: *mut c_void, operator_id: OperatorId) -> Self {
		let vtable = descriptor.vtable;
		let capabilities = from_bitmask(descriptor.capabilities).into_boxed_slice();

		Self {
			capabilities,
			vtable,
			instance,
			operator_id,
			builder_registry: BuilderRegistry::new(),
		}
	}
}

// SAFETY: ExternCOperatorHandle is only accessed from a single actor at a time.
unsafe impl Send for ExternCOperatorHandle {}

impl Drop for ExternCOperatorHandle {
	fn drop(&mut self) {
		if !self.instance.is_null() {
			unsafe { (self.vtable.destroy)(self.instance) };
		}
	}
}

#[inline]
#[instrument(name = "flow::extern_c::marshal", level = "trace", skip_all)]
fn marshal_input(arena: &mut Arena, change: &Change) -> ExternCChange {
	arena.marshal_change(change)
}

#[inline]
#[instrument(name = "flow::extern_c::vtable_call", level = "trace", skip_all, fields(operator_id = operator_id.0))]
fn call_vtable(
	vtable: &ExternCOperatorVTable,
	instance: *mut c_void,
	extern_c_ctx_ptr: *mut ExternCContextRaw,
	extern_c_input: &ExternCChange,
	operator_id: OperatorId,
) -> i32 {
	// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
	// ExternCOperatorHandle::drop calls destroy; extern_c_ctx_ptr and extern_c_input point at caller-owned values
	// that outlive the call, and the host holds no Rust borrow of either while the guest runs.
	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		(vtable.apply)(instance, extern_c_ctx_ptr, extern_c_input)
	}));

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
			error!(operator_id = operator_id.0, "extern-C operator panicked during apply: {}", msg);
			abort();
		}
	}
}

impl HostOperator for ExternCOperatorHandle {
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

	#[instrument(name = "flow::extern_c::apply", level = "trace", skip_all, fields(
		operator_id = self.operator_id.0,
		input_diff_count = change.diffs.len(),
		output_diff_count = field::Empty
	))]
	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		// SAFETY: the arena is thread-local and the previous apply's guest call has returned, so
		// no pointer into it is still live when it is cleared and re-borrowed.
		EXTERN_C_MARSHAL_ARENA.with(|cell| unsafe { (*cell.get()).clear() });
		let extern_c_input =
			EXTERN_C_MARSHAL_ARENA.with(|cell| marshal_input(unsafe { &mut *cell.get() }, &change));

		let version = change.version;
		let changed_at = change.changed_at;

		let mut host_ctx = ExternCHostContext::new(host);
		let mut extern_c_ctx = new_extern_c_context(&mut host_ctx, self.operator_id, create_host_callbacks());
		let extern_c_ctx_ptr = &raw mut extern_c_ctx;

		let result_code = with_registry(&self.builder_registry, || {
			call_vtable(&self.vtable, self.instance, extern_c_ctx_ptr, &extern_c_input, self.operator_id)
		});

		if result_code != 0 {
			let _ = self.builder_registry.drain();
			return Err(SdkError::Other(format!(
				"extern-C operator apply failed with code: {}",
				result_code
			))
			.into());
		}

		let output_change = drain_emitted_diffs(&self.builder_registry, self.operator_id, version, changed_at);

		Span::current().record("output_diff_count", output_change.diffs.len());

		Ok(output_change)
	}

	#[instrument(name = "flow::extern_c::on_timer", level = "trace", skip_all, fields(
		operator_id = self.operator_id.0,
		output_diff_count = field::Empty
	))]
	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		let version = host.version();
		let key = timer.key.as_ref();

		let mut host_ctx = ExternCHostContext::new(host);
		let mut extern_c_ctx = new_extern_c_context(&mut host_ctx, self.operator_id, create_host_callbacks());
		let extern_c_ctx_ptr = &raw mut extern_c_ctx;

		// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
		// Drop calls destroy; extern_c_ctx_ptr is a local ExternCContextRaw with no Rust borrow of it live
		// during the call, and key's ptr/len describe a slice of `timer`, which outlives the call.
		let result_code = self.invoke_under_panic_guard("on_timer", || unsafe {
			(self.vtable.on_timer)(
				self.instance,
				extern_c_ctx_ptr,
				timer.at.to_millis(),
				timer.kind as u8,
				key.as_ptr(),
				key.len(),
			)
		});

		if result_code < 0 {
			let _ = self.builder_registry.drain();
			return Err(SdkError::Other(format!(
				"extern-C operator on_timer failed with code: {}",
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
		let mut usage = ExternCStateUsage::default();
		// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
		// Drop calls destroy; `usage` is an initialised local, exclusively borrowed for the call.
		match unsafe { (self.vtable.sample)(self.instance, &mut usage) } {
			EXTERN_C_OK => Some(sample_from_usage(&usage)),
			EXTERN_C_SAMPLE_NO_DATA => None,
			code => {
				error!(
					operator_id = self.operator_id.0,
					code, "extern-C operator failed to report state usage"
				);
				None
			}
		}
	}
}

fn sample_from_usage(usage: &ExternCStateUsage) -> OperatorSample {
	let state = StateMemory::new(Count::new(usage.state_entries), ByteSize::from_bytes(usage.state_bytes));
	let row_numbers =
		StateMemory::new(Count::new(usage.row_number_entries), ByteSize::from_bytes(usage.row_number_bytes));
	OperatorSample::with_memory(state).with_row_number_cache(row_numbers)
}

impl ExternCOperatorHandle {
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
						"extern-C operator panicked during {}: {}", op, msg
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
	use reifydb_sdk::flow::operator::extern_c::wire::state::ExternCStateUsage;

	use super::sample_from_usage;

	#[test]
	fn host_sample_decode_keeps_the_row_number_cache_off_the_state_total() {
		// The two pairs cross on separate fields; folding either into the other double-counts it.
		let usage = ExternCStateUsage {
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
