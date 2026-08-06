// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::{self, Any},
	backtrace::Backtrace,
	cell::RefCell,
	ffi::c_void,
	io::{self, Write},
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
	slice,
};

use reifydb_abi::{
	constants::{FFI_ERROR_NULL_PTR, FFI_OK, FFI_SAMPLE_NO_DATA},
	context::context::ContextFFI,
	data::state::StateUsageFFI,
	flow::{
		change::ChangeFFI,
		diff::{DiffFFI, DiffType},
	},
	operator::{timer::TimerKind, vtable::OperatorVTableFFI},
};
use reifydb_core::metrics::heap::OperatorSample;
use reifydb_value::value::datetime::DateTime;
use tracing::{error, instrument, warn};

use crate::operator::{FFIOperator, change::BorrowedChange, context::ffi::FFIOperatorContext, timer::Timer};

thread_local! {



	static FFI_FATAL_DETAIL: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn set_fatal_detail(detail: String) {
	FFI_FATAL_DETAIL.with(|cell| *cell.borrow_mut() = Some(detail));
}

fn take_fatal_detail() -> Option<String> {
	FFI_FATAL_DETAIL.with(|cell| cell.borrow_mut().take())
}

fn origin_type_name(origin_type: u8) -> &'static str {
	match origin_type {
		0 => "Flow",
		1 => "Table",
		2 => "View",
		3 => "VTable",
		4 => "RingBuffer",
		6 => "Dictionary",
		7 => "Series",
		_ => "Unknown",
	}
}

/// # Safety
///
/// `input` must be null or valid for reads of one initialised, aligned
/// `ChangeFFI`, whose `diffs` must in turn be null or valid for reads of
/// `diff_count` initialised, aligned `DiffFFI`. Both must outlive the call.
unsafe fn describe_change_input(input: *const ChangeFFI) -> String {
	if input.is_null() {
		return "<null>".to_string();
	}
	// SAFETY: input was checked non-null above and this fn's contract makes it one initialised, aligned ChangeFFI
	// that outlives the call.
	let ffi = unsafe { &*input };
	let types = if !ffi.diffs.is_null() && ffi.diff_count > 0 {
		// SAFETY: this fn's contract makes diffs cover diff_count initialised, aligned DiffFFI; non-null and a
		// non-zero count are checked in this branch's condition.
		let diffs: &[DiffFFI] = unsafe { slice::from_raw_parts(ffi.diffs, ffi.diff_count) };
		let names: Vec<&'static str> = diffs
			.iter()
			.map(|d| match d.diff_type {
				DiffType::Insert => "Insert",
				DiffType::Update => "Update",
				DiffType::Remove => "Remove",
			})
			.collect();
		format!("[{}]", names.join(", "))
	} else {
		"[]".to_string()
	};
	format!(
		"origin={}({}) diff_count={} diff_types={}",
		origin_type_name(ffi.origin.origin),
		ffi.origin.id,
		ffi.diff_count,
		types,
	)
}

fn describe_panic_payload(payload: &Box<dyn Any + Send>) -> String {
	if let Some(s) = payload.downcast_ref::<&'static str>() {
		s.to_string()
	} else if let Some(s) = payload.downcast_ref::<String>() {
		s.clone()
	} else {
		format!("<non-string panic payload, TypeId={:?}>", (**payload).type_id())
	}
}

fn code_meaning(code: i32) -> &'static str {
	match code {
		-2 => "operator returned Err",
		-3 => "unmarshal failed",
		-99 => "panic caught in catch_unwind",
		_ => "unknown",
	}
}

fn print_ffi_fatal(
	entry: &str,
	operator: &str,
	code: i32,
	detail: &str,
	input_description: Option<&str>,
	backtrace: Option<&Backtrace>,
) {
	let mut err = io::stderr().lock();
	let _ = writeln!(err, "========== FFI FATAL ==========");
	let _ = writeln!(err, "entry:    {}", entry);
	let _ = writeln!(err, "operator: {}", operator);
	let _ = writeln!(err, "code:     {} ({})", code, code_meaning(code));
	let _ = writeln!(
		err,
		"detail:   {}",
		if detail.is_empty() {
			"<none>"
		} else {
			detail
		}
	);
	if let Some(desc) = input_description {
		let _ = writeln!(err, "input:    {}", desc);
	}
	if let Some(bt) = backtrace {
		let _ = writeln!(err, "backtrace:\n{}", bt);
	}
	let _ = writeln!(err, "===============================");
	let _ = err.flush();
}

pub struct OperatorWrapper<O: FFIOperator> {
	pub(crate) operator: O,
}

impl<O: FFIOperator> OperatorWrapper<O> {
	pub fn new(operator: O) -> Self {
		Self {
			operator,
		}
	}

	pub fn as_ptr(&mut self) -> *mut c_void {
		self as *mut _ as *mut c_void
	}

	pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
		unsafe { &mut *(ptr as *mut Self) }
	}
}

/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>` created by `Box::new`.
/// - `ctx` must be a valid pointer to a `ContextFFI`.
/// - `input` must be a valid pointer to a `ChangeFFI` whose buffer pointers are valid for the duration of the call.
#[instrument(name = "flow::operator::ffi::apply", level = "debug", skip_all, fields(
	operator_type = any::type_name::<O>(),
))]
pub unsafe extern "C" fn ffi_apply<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	input: *const ChangeFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		if input.is_null() {
			set_fatal_detail("ffi_apply: input is null".to_string());
			return -3;
		}
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);
		// SAFETY: discharges BorrowedChange::from_raw; input was checked non-null above and ffi_apply's
		// contract keeps the ChangeFFI and its buffers live for the borrow, which ends with this closure.
		let borrowed = unsafe { BorrowedChange::from_raw(input) };
		let mut op_ctx = FFIOperatorContext::new(ctx);
		match wrapper.operator.apply(&mut op_ctx, borrowed) {
			Ok(()) => 0,
			Err(e) => {
				warn!(?e, "Apply failed");
				set_fatal_detail(format!("{:?}", e));
				-2
			}
		}
	}));

	let (code, backtrace) = match result {
		Ok(code) => (code, None),
		Err(payload) => {
			let bt = Backtrace::force_capture();
			set_fatal_detail(describe_panic_payload(&payload));
			error!("Panic in ffi_apply");
			(-99, Some(bt))
		}
	};

	if code < 0 {
		let detail = take_fatal_detail().unwrap_or_default();
		// SAFETY: discharges describe_change_input; ffi_apply's contract holds for input and its diffs array
		// for the whole call, and null is handled inside.
		let input_desc = unsafe { describe_change_input(input) };
		print_ffi_fatal(
			"ffi_apply",
			any::type_name::<O>(),
			code,
			&detail,
			Some(&input_desc),
			backtrace.as_ref(),
		);
		abort();
	}
	code
}

/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>` created by `Box::new`.
/// - `ctx` must be a valid pointer to a `ContextFFI`.
#[instrument(name = "flow::operator::ffi::on_timer", level = "debug", skip_all, fields(
	operator_type = any::type_name::<O>(),
))]
pub unsafe extern "C" fn ffi_on_timer<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	at_millis: u64,
	kind: u8,
	key: *const u8,
	key_len: usize,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);

		let Some(kind) = TimerKind::from_u8(kind) else {
			set_fatal_detail(format!("host fired a timer with unknown kind {}", kind));
			return -2;
		};
		let timer = Timer {
			at: DateTime::from_millis(at_millis),
			kind,
			key: if key.is_null() || key_len == 0 {
				&[]
			} else {
				// SAFETY: null and zero-length are handled by the other arm; otherwise the host
				// caller keeps key readable for key_len bytes for the duration of this call.
				unsafe { slice::from_raw_parts(key, key_len) }
			},
		};
		let mut op_ctx = FFIOperatorContext::new(ctx);

		match wrapper.operator.on_timer(&mut op_ctx, timer) {
			Ok(()) => 0,
			Err(e) => {
				warn!(?e, "on_timer failed");
				set_fatal_detail(format!("{:?}", e));
				-2
			}
		}
	}));

	let (code, backtrace) = match result {
		Ok(code) => (code, None),
		Err(payload) => {
			let bt = Backtrace::force_capture();
			set_fatal_detail(describe_panic_payload(&payload));
			error!("Panic in ffi_on_timer");
			(-99, Some(bt))
		}
	};

	if code < 0 {
		let detail = take_fatal_detail().unwrap_or_default();
		let input_desc = format!("at_millis={} kind={} key_len={}", at_millis, kind, key_len);
		print_ffi_fatal(
			"ffi_on_timer",
			any::type_name::<O>(),
			code,
			&detail,
			Some(&input_desc),
			backtrace.as_ref(),
		);
		abort();
	}
	code
}

/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>` originally created by `Box::new`.
pub unsafe extern "C" fn ffi_seal_after_ms<O: FFIOperator>(instance: *mut c_void) -> u64 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);
		wrapper.operator.seal_after_ms().unwrap_or(0)
	}));

	match result {
		Ok(span) => span,
		Err(payload) => {
			let bt = Backtrace::force_capture();
			let detail = describe_panic_payload(&payload);
			error!("Panic in ffi_seal_after_ms - aborting");
			print_ffi_fatal("ffi_seal_after_ms", any::type_name::<O>(), -99, &detail, None, Some(&bt));
			abort();
		}
	}
}

/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>` originally created by `Box::new`, or null (in which
///   case this is a no-op).
pub unsafe extern "C" fn ffi_destroy<O: FFIOperator>(instance: *mut c_void) {
	if instance.is_null() {
		return;
	}

	// SAFETY: instance was checked non-null above and ffi_destroy's contract makes it a Box::new-allocated
	// OperatorWrapper<O>; the host calls destroy once, so ownership is taken exactly once.
	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		let _wrapper = Box::from_raw(instance as *mut OperatorWrapper<O>);
	}));

	if let Err(payload) = result {
		let bt = Backtrace::force_capture();
		let detail = describe_panic_payload(&payload);
		error!("Panic in ffi_destroy - aborting");
		print_ffi_fatal("ffi_destroy", any::type_name::<O>(), -99, &detail, None, Some(&bt));
		abort();
	}
}

/// Called once per transaction at commit time, so this is the only point at which guest state reaches the host.
///
/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>`.
/// - `ctx` must point to a valid `ContextFFI` for the duration of the call.
pub unsafe extern "C" fn ffi_flush_state<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	usage: *mut StateUsageFFI,
) -> i32 {
	if instance.is_null() || ctx.is_null() || usage.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	let result = catch_unwind(AssertUnwindSafe(|| {
		// SAFETY: instance was checked non-null above and ffi_flush_state's contract makes it a live,
		// aligned OperatorWrapper<O>; this is the only borrow of it taken in this call.
		let wrapper = unsafe { &mut *(instance as *mut OperatorWrapper<O>) };
		let mut op_ctx = FFIOperatorContext::new(ctx);
		let outcome = wrapper.operator.flush_state(&mut op_ctx);
		let report = wrapper.operator.sample();
		(outcome, report)
	}));

	match result {
		Ok((Ok(()), None)) => FFI_SAMPLE_NO_DATA,
		Ok((Ok(()), report)) => {
			// SAFETY: usage was null-checked above and the caller guarantees it is aligned and writable
			// for a StateUsageFFI for the duration of this call.
			unsafe {
				*usage = usage_from_sample(report);
			}
			FFI_OK
		}
		Ok((Err(e), _)) => {
			error!("operator flush_state failed - aborting");
			print_ffi_fatal("ffi_flush_state", any::type_name::<O>(), -2, &format!("{:?}", e), None, None);
			abort();
		}
		Err(payload) => {
			let bt = Backtrace::force_capture();
			let detail = describe_panic_payload(&payload);
			error!("Panic in ffi_flush_state - aborting");
			print_ffi_fatal("ffi_flush_state", any::type_name::<O>(), -99, &detail, None, Some(&bt));
			abort();
		}
	}
}

/// Declining to report is legitimate: `FFI_SAMPLE_NO_DATA` leaves `out` untouched rather than writing zeroes.
///
/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>`.
/// - `out` must be a valid, writable, aligned pointer to a `StateUsageFFI`.
pub unsafe extern "C" fn ffi_sample<O: FFIOperator>(instance: *mut c_void, out: *mut StateUsageFFI) -> i32 {
	if instance.is_null() || out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);
		wrapper.operator.sample()
	}));

	match result {
		Ok(None) => FFI_SAMPLE_NO_DATA,
		Ok(Some(report)) => {
			// SAFETY: out was null-checked above and the caller guarantees it is aligned and writable for
			// a StateUsageFFI for the duration of this call.
			unsafe {
				*out = usage_from_sample(Some(report));
			}
			FFI_OK
		}
		Err(payload) => {
			let bt = Backtrace::force_capture();
			let detail = describe_panic_payload(&payload);
			error!("Panic in ffi_sample - aborting");
			print_ffi_fatal("ffi_sample", any::type_name::<O>(), -99, &detail, None, Some(&bt));
			abort();
		}
	}
}

fn usage_from_sample(sample: Option<OperatorSample>) -> StateUsageFFI {
	let mut usage = StateUsageFFI::default();
	if let Some(sample) = sample {
		if let Some(memory) = sample.memory {
			usage.state_entries = memory.entries.as_u64();
			usage.state_bytes = memory.bytes.as_bytes();
		}
		if let Some(rows) = sample.row_number_cache {
			usage.row_number_entries = rows.entries.as_u64();
			usage.row_number_bytes = rows.bytes.as_bytes();
		}
		if let Some(membership) = sample.membership {
			usage.has_membership = 1;
			usage.membership_entries = membership.entries.as_u64();
			usage.membership_bytes = membership.bytes.as_bytes();
		}
		if let Some(completeness) = sample.completeness {
			usage.has_completeness = 1;
			usage.values_complete = completeness.values_complete as u64;
			usage.membership_complete = completeness.membership_complete as u64;
			usage.absences_served = completeness.absences_served.as_u64();
			usage.false_positives = completeness.false_positives.as_u64();
			usage.revocations = completeness.revocations.as_u64();
		}
		if let Some(pool) = sample.pool {
			usage.has_pool = 1;
			usage.pool_budget = pool.budget.as_bytes();
			usage.pool_evictions = pool.evictions.as_u64();
		}
	}
	usage
}

pub fn create_vtable<O: FFIOperator>() -> OperatorVTableFFI {
	OperatorVTableFFI {
		apply: ffi_apply::<O>,
		on_timer: ffi_on_timer::<O>,
		destroy: ffi_destroy::<O>,
		flush_state: ffi_flush_state::<O>,
		sample: ffi_sample::<O>,
		seal_after_ms: ffi_seal_after_ms::<O>,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::metrics::heap::{StateCompleteness, StateMemory, StatePool};
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::*;

	fn memory(entries: u64, bytes: u64) -> StateMemory {
		StateMemory::new(Count::new(entries), ByteSize::from_bytes(bytes))
	}

	#[test]
	fn guest_usage_reports_the_total_once_and_never_adds_the_dirty_subset() {
		// The reported total already contains the dirty subset, and the host charges whatever crosses the
		// boundary straight to the lease, so folding dirty in again bills it twice.
		let sample = OperatorSample::with_memory(memory(10, 4096)).with_dirty_memory(memory(4, 1024));

		let usage = usage_from_sample(Some(sample));

		assert_eq!(usage.state_bytes, 4096, "the guest ships the reported total, once");
		assert_eq!(usage.state_entries, 10);
	}

	#[test]
	fn guest_usage_carries_the_row_number_cache_on_its_own_fields() {
		let sample = OperatorSample::with_memory(memory(10, 4096)).with_row_number_cache(memory(2, 512));

		let usage = usage_from_sample(Some(sample));

		assert_eq!(usage.state_bytes, 4096);
		assert_eq!(usage.state_entries, 10);
		assert_eq!(usage.row_number_bytes, 512);
		assert_eq!(usage.row_number_entries, 2);
	}

	#[test]
	fn guest_usage_carries_membership_and_completeness_behind_presence_flags() {
		// The flags separate "not reported" from "all zero": a pre-hydration operator ships neither, and
		// without them the host renders every such operator as a degraded values_complete=0 gauge.
		let bare = usage_from_sample(Some(OperatorSample::with_memory(memory(1, 64))));
		assert_eq!(bare.has_membership, 0, "an unreported membership slot must not claim presence");
		assert_eq!(bare.has_completeness, 0);

		let sample = OperatorSample::with_memory(memory(1, 64))
			.with_membership(memory(7, 320))
			.with_completeness(StateCompleteness {
				values_complete: false,
				membership_complete: true,
				absences_served: Count::new(9),
				false_positives: Count::new(1),
				revocations: Count::new(2),
			});
		let usage = usage_from_sample(Some(sample));
		assert_eq!(usage.has_membership, 1);
		assert_eq!(usage.membership_entries, 7);
		assert_eq!(usage.membership_bytes, 320);
		assert_eq!(usage.has_completeness, 1);
		assert_eq!(usage.values_complete, 0);
		assert_eq!(usage.membership_complete, 1);
		assert_eq!(usage.absences_served, 9);
		assert_eq!(usage.false_positives, 1);
		assert_eq!(usage.revocations, 2);
	}

	#[test]
	fn an_operator_reporting_nothing_yields_a_zeroed_usage() {
		let usage = usage_from_sample(None);

		assert_eq!(usage.state_bytes, 0);
		assert_eq!(usage.state_entries, 0);
		assert_eq!(usage.row_number_bytes, 0);
		assert_eq!(usage.row_number_entries, 0);
	}

	#[test]
	fn guest_usage_carries_the_private_pool_behind_a_presence_flag() {
		// A guest's private pool is invisible to the host, so this is the only channel reporting the budget
		// it actually enforced; the flag keeps an absent report from rendering as a real 0-byte budget.
		let bare = usage_from_sample(Some(OperatorSample::with_memory(memory(1, 64))));
		assert_eq!(bare.has_pool, 0, "an unreported pool must not claim presence");

		let sample = OperatorSample::with_memory(memory(1, 64)).with_pool(StatePool {
			budget: ByteSize::from_bytes(8 * 1024 * 1024),
			evictions: Count::new(3),
		});
		let usage = usage_from_sample(Some(sample));
		assert_eq!(usage.has_pool, 1);
		assert_eq!(usage.pool_budget, 8 * 1024 * 1024);
		assert_eq!(usage.pool_evictions, 3);
	}
}
