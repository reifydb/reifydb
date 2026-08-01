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
	key::operator_group_state::GroupSet,
	metrics::heap::{OperatorSample, StateCompleteness, StateMemory, StatePool},
	state::budget::{LeaseGrant, LeaseReport, OperatorStateBudgetHandle},
	value::column::columns::Columns,
};
use reifydb_engine::vm::executor::Executor;
use reifydb_extension::ffi_callbacks::builder::{BuilderRegistry, with_registry};
use reifydb_flow::{
	operator::Reclaimable,
	timer::Timer,
	transaction::{
		FlowTransaction,
		slot::{PersistFn, zero_usage},
	},
};
use reifydb_sdk::{error::SdkError, ffi::arena::Arena};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::{Span, error, field, instrument};

use crate::{
	engine::lease_demand,
	ffi::{callbacks::create_host_callbacks, context::new_ffi_context},
	operator::{Operator, scale_from_millis, sealed_or_idle},
};

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

	state_budget: OperatorStateBudgetHandle,
}

impl FFIOperatorHandle {
	pub fn new(
		descriptor: OperatorDescriptorFFI,
		instance: *mut c_void,
		operator_id: OperatorId,
		executor: Executor,
		state_budget: OperatorStateBudgetHandle,
		lease: LeaseGrant,
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
				clock_now_nanos: 0,
				state_lease_bytes: lease.bytes().as_bytes(),
				callbacks: create_host_callbacks(),
			}),
			state_budget,
		}
	}

	fn ensure_txn_setup(&self, txn: &mut FlowTransaction) -> Result<()> {
		let txn_version = txn.version().0;
		if self.last_registered_txn.get() != txn_version {
			ensure_flush_slot(txn, self.operator_id, self.vtable, self.instance, self.executor.clone())?;
			self.last_registered_txn.set(txn_version);
			// SAFETY: one actor drives this operator and no guest call is in flight here, so
			// the context cell is not aliased while this &mut exists.
			let ctx = unsafe { &mut *self.cached_ctx.get() };
			ctx.txn_ptr = txn as *mut _ as *mut c_void;
			ctx.executor_ptr = &self.executor as *const _ as *const c_void;
			ctx.clock_now_nanos = txn.clock().now().to_nanos();
			ctx.state_lease_bytes = self
				.state_budget
				.current_lease(self.operator_id)
				.map(|lease| lease.grant.bytes().as_bytes())
				.unwrap_or(0);
		}
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
	txn: &mut FlowTransaction,
	operator_id: OperatorId,
	vtable: OperatorVTableFFI,
	instance: *mut c_void,
	executor: Executor,
) -> Result<()> {
	let send_instance = SendableInstance(instance);
	let _ = txn.operator_state(operator_id, zero_usage, move |_txn| {
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
				Ok(FFI_OK) => {
					let report = lease_report_from_usage(&usage);
					let budget = txn.state_budget();
					budget.report_lease(captured_id, report);
					budget.resize_lease_to_demand(captured_id, lease_demand(&report));
					Ok(())
				}
				Ok(FFI_SAMPLE_NO_DATA) => {
					txn.state_budget().report_lease_none(captured_id);
					Ok(())
				}
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

	fn retention_scale(&self) -> Option<Duration> {
		// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
		// Drop calls destroy; the call passes no host pointers.
		scale_from_millis(Some(unsafe { (self.vtable.seal_after_ms)(self.instance) }))
	}

	fn reclaimable_through(&self, txn: &mut FlowTransaction, watermark: DateTime) -> Result<Reclaimable> {
		sealed_or_idle(txn, self.operator_id, watermark, self.retention_scale())
	}

	#[instrument(name = "flow::ffi::apply", level = "trace", skip_all, fields(
		operator_id = self.operator_id.0,
		input_diff_count = change.diffs.len(),
		output_diff_count = field::Empty
	))]
	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
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
	fn on_timer(&self, txn: &mut FlowTransaction, timer: Timer) -> Result<Option<Change>> {
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

	fn invalidate_groups(&self, groups: &GroupSet) {
		if groups.is_empty() || !self.capabilities.contains(&OperatorCapability::Reclaim) {
			return;
		}
		let (ptr, len) = groups.as_raw_parts();
		// SAFETY: vtable and instance come from the descriptor of the loaded operator and stay valid until
		// Drop calls destroy; ptr/len are the raw parts of `groups`, borrowed for the whole call, so that
		// range stays readable throughout.
		let code = self.invoke_under_panic_guard("invalidate_groups", || unsafe {
			(self.vtable.invalidate_groups)(self.instance, ptr, len)
		});
		if code != FFI_OK {
			error!(
				operator_id = self.operator_id.0,
				code, "FFI operator failed to invalidate reclaimed groups"
			);
		}
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

fn lease_report_from_usage(usage: &StateUsageFFI) -> LeaseReport {
	LeaseReport {
		state: StateMemory::new(Count::new(usage.state_entries), ByteSize::from_bytes(usage.state_bytes)),
		row_numbers: StateMemory::new(
			Count::new(usage.row_number_entries),
			ByteSize::from_bytes(usage.row_number_bytes),
		),
	}
}

fn sample_from_usage(usage: &StateUsageFFI) -> OperatorSample {
	let report = lease_report_from_usage(usage);
	let mut sample = OperatorSample::with_memory(report.state).with_row_number_cache(report.row_numbers);
	if usage.has_membership != 0 {
		sample = sample.with_membership(StateMemory::new(
			Count::new(usage.membership_entries),
			ByteSize::from_bytes(usage.membership_bytes),
		));
	}
	if usage.has_completeness != 0 {
		sample = sample.with_completeness(StateCompleteness {
			values_complete: usage.values_complete != 0,
			membership_complete: usage.membership_complete != 0,
			absences_served: Count::new(usage.absences_served),
			false_positives: Count::new(usage.false_positives),
			revocations: Count::new(usage.revocations),
		});
	}
	if usage.has_pool != 0 {
		sample = sample.with_pool(StatePool {
			budget: ByteSize::from_bytes(usage.pool_budget),
			evictions: Count::new(usage.pool_evictions),
		});
	}
	sample
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
	fn host_sample_decode_mirrors_the_guest_encoding_including_presence_flags() {
		// Presence flags gate the optional slots, so a dylib that reported no membership data
		// must not surface as a degraded node.
		let mut usage = StateUsageFFI {
			state_entries: 3,
			state_bytes: 128,
			row_number_entries: 2,
			row_number_bytes: 64,
			..StateUsageFFI::default()
		};
		let bare = sample_from_usage(&usage);
		assert!(bare.membership.is_none(), "flag zero must decode as not-reported, not as zeros");
		assert!(bare.completeness.is_none());
		assert_eq!(bare.memory.expect("state memory always ships").entries.as_u64(), 3);

		usage.has_membership = 1;
		usage.membership_entries = 7;
		usage.membership_bytes = 320;
		usage.has_completeness = 1;
		usage.values_complete = 0;
		usage.membership_complete = 1;
		usage.absences_served = 9;
		usage.false_positives = 1;
		usage.revocations = 2;
		let full = sample_from_usage(&usage);
		let membership = full.membership.expect("flagged membership must decode");
		assert_eq!(membership.entries.as_u64(), 7);
		assert_eq!(membership.bytes.as_bytes(), 320);
		let completeness = full.completeness.expect("flagged completeness must decode");
		assert!(!completeness.values_complete);
		assert!(completeness.membership_complete);
		assert_eq!(completeness.absences_served.as_u64(), 9);
		assert_eq!(completeness.false_positives.as_u64(), 1);
		assert_eq!(completeness.revocations.as_u64(), 2);
	}

	#[test]
	fn host_sample_decode_surfaces_the_guest_pool_behind_its_presence_flag() {
		// A guest's private pool is invisible to the host, so without this decode a dylib
		// operator's revocations have no attributable budget in the memory log.
		let mut usage = StateUsageFFI {
			state_entries: 3,
			state_bytes: 128,
			..StateUsageFFI::default()
		};
		assert!(sample_from_usage(&usage).pool.is_none(), "flag zero must decode as not-reported");

		usage.has_pool = 1;
		usage.pool_budget = 8 * 1024 * 1024;
		usage.pool_evictions = 5;
		let pool = sample_from_usage(&usage).pool.expect("flagged pool must decode");
		assert_eq!(pool.budget.as_bytes(), 8 * 1024 * 1024);
		assert_eq!(pool.evictions.as_u64(), 5);
	}
}
