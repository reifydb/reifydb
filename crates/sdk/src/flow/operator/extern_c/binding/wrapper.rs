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

use reifydb_core::{interface::change::DiffType, metrics::heap::OperatorSample, state::store::TimerKind};
use reifydb_flow::operator::state::seal::coord::Coord;
use reifydb_value::value::datetime::DateTime;
use tracing::{error, instrument, warn};

use crate::{
	common::extern_c::wire::status::{EXTERN_C_ERROR_NULL_PTR, EXTERN_C_OK, EXTERN_C_SAMPLE_NO_DATA},
	flow::{
		extern_c::wire::change::{ExternCChange, ExternCDiff},
		operator::{
			change::BorrowedChange,
			extern_c::{
				binding::{context::ExternCContext, operator::ExternCOperator},
				wire::{
					context::ExternCContextRaw, state::ExternCStateUsage,
					vtable::ExternCOperatorVTable,
				},
			},
			timer::Timer,
		},
	},
};

thread_local! {
	static EXTERN_C_FATAL_DETAIL: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn set_fatal_detail(detail: String) {
	EXTERN_C_FATAL_DETAIL.with(|cell| *cell.borrow_mut() = Some(detail));
}

fn take_fatal_detail() -> Option<String> {
	EXTERN_C_FATAL_DETAIL.with(|cell| cell.borrow_mut().take())
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
/// `ExternCChange`, whose `diffs` must in turn be null or valid for reads of
/// `diff_count` initialised, aligned `ExternCDiff`. Both must outlive the call.
unsafe fn describe_change_input(input: *const ExternCChange) -> String {
	if input.is_null() {
		return "<null>".to_string();
	}
	// SAFETY: input was checked non-null above and this fn's contract makes it one initialised, aligned
	// ExternCChange that outlives the call.
	let change = unsafe { &*input };
	let types = if !change.diffs.is_null() && change.diff_count > 0 {
		// SAFETY: this fn's contract makes diffs cover diff_count initialised, aligned ExternCDiff; non-null
		// and a non-zero count are checked in this branch's condition.
		let diffs: &[ExternCDiff] = unsafe { slice::from_raw_parts(change.diffs, change.diff_count) };
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
		origin_type_name(change.origin.origin),
		change.origin.id,
		change.diff_count,
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

fn print_extern_c_fatal(
	entry: &str,
	operator: &str,
	code: i32,
	detail: &str,
	input_description: Option<&str>,
	backtrace: Option<&Backtrace>,
) {
	let mut err = io::stderr().lock();
	let _ = writeln!(err, "======= EXTERN C FATAL ========");
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

pub struct OperatorWrapper<O: ExternCOperator> {
	pub(crate) operator: O,
}

impl<O: ExternCOperator> OperatorWrapper<O> {
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
/// - `ctx` must be a valid pointer to a `ExternCContextRaw`.
/// - `input` must be a valid pointer to a `ExternCChange` whose buffer pointers are valid for the duration of the call.
#[instrument(name = "flow::operator::extern_c::apply", level = "debug", skip_all, fields(
	operator_type = any::type_name::<O>(),
))]
pub unsafe extern "C" fn extern_c_apply<O: ExternCOperator>(
	instance: *mut c_void,
	ctx: *mut ExternCContextRaw,
	input: *const ExternCChange,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		if input.is_null() {
			set_fatal_detail("extern_c_apply: input is null".to_string());
			return -3;
		}
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);
		// SAFETY: discharges BorrowedChange::from_raw; input was checked non-null above and extern_c_apply's
		// contract keeps the ExternCChange and its buffers live for the borrow, which ends with this closure.
		let borrowed = unsafe { BorrowedChange::from_raw(input) };
		let mut op_ctx = ExternCContext::new(ctx);
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
			error!("Panic in extern_c_apply");
			(-99, Some(bt))
		}
	};

	if code < 0 {
		let detail = take_fatal_detail().unwrap_or_default();
		// SAFETY: discharges describe_change_input; extern_c_apply's contract holds for input and its diffs
		// array for the whole call, and null is handled inside.
		let input_desc = unsafe { describe_change_input(input) };
		print_extern_c_fatal(
			"extern_c_apply",
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
/// - `ctx` must be a valid pointer to a `ExternCContextRaw`.
#[instrument(name = "flow::operator::extern_c::on_timer", level = "debug", skip_all, fields(
	operator_type = any::type_name::<O>(),
))]
pub unsafe extern "C" fn extern_c_on_timer<O: ExternCOperator>(
	instance: *mut c_void,
	ctx: *mut ExternCContextRaw,
	due_bits: u64,
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
			due: DateTime::from_bits(due_bits),
			kind,
			key: if key.is_null() || key_len == 0 {
				&[]
			} else {
				// SAFETY: null and zero-length are handled by the other arm; otherwise the host
				// caller keeps key readable for key_len bytes for the duration of this call.
				unsafe { slice::from_raw_parts(key, key_len) }
			},
		};
		let mut op_ctx = ExternCContext::new(ctx);

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
			error!("Panic in extern_c_on_timer");
			(-99, Some(bt))
		}
	};

	if code < 0 {
		let detail = take_fatal_detail().unwrap_or_default();
		let input_desc = format!("due_bits={} kind={} key_len={}", due_bits, kind, key_len);
		print_extern_c_fatal(
			"extern_c_on_timer",
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
pub unsafe extern "C" fn extern_c_lateness_ms<O: ExternCOperator>(instance: *mut c_void) -> u64 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);
		wrapper.operator.lateness().and_then(<DateTime as Coord>::span_millis).unwrap_or(0)
	}));

	match result {
		Ok(span) => span,
		Err(payload) => {
			let bt = Backtrace::force_capture();
			let detail = describe_panic_payload(&payload);
			error!("Panic in extern_c_lateness_ms - aborting");
			print_extern_c_fatal(
				"extern_c_lateness_ms",
				any::type_name::<O>(),
				-99,
				&detail,
				None,
				Some(&bt),
			);
			abort();
		}
	}
}

/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>` originally created by `Box::new`, or null (in which
///   case this is a no-op).
pub unsafe extern "C" fn extern_c_destroy<O: ExternCOperator>(instance: *mut c_void) {
	if instance.is_null() {
		return;
	}

	// SAFETY: instance was checked non-null above and extern_c_destroy's contract makes it a Box::new-allocated
	// OperatorWrapper<O>; the host calls destroy once, so ownership is taken exactly once.
	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		let _wrapper = Box::from_raw(instance as *mut OperatorWrapper<O>);
	}));

	if let Err(payload) = result {
		let bt = Backtrace::force_capture();
		let detail = describe_panic_payload(&payload);
		error!("Panic in extern_c_destroy - aborting");
		print_extern_c_fatal("extern_c_destroy", any::type_name::<O>(), -99, &detail, None, Some(&bt));
		abort();
	}
}

/// Declining to report is legitimate: `EXTERN_C_SAMPLE_NO_DATA` leaves `out` untouched rather than writing zeroes.
///
/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>`.
/// - `out` must be a valid, writable, aligned pointer to a `ExternCStateUsage`.
pub unsafe extern "C" fn extern_c_sample<O: ExternCOperator>(
	instance: *mut c_void,
	out: *mut ExternCStateUsage,
) -> i32 {
	if instance.is_null() || out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);
		wrapper.operator.sample()
	}));

	match result {
		Ok(None) => EXTERN_C_SAMPLE_NO_DATA,
		Ok(Some(report)) => {
			// SAFETY: out was null-checked above and the caller guarantees it is aligned and writable for
			// a ExternCStateUsage for the duration of this call.
			unsafe {
				*out = usage_from_sample(Some(report));
			}
			EXTERN_C_OK
		}
		Err(payload) => {
			let bt = Backtrace::force_capture();
			let detail = describe_panic_payload(&payload);
			error!("Panic in extern_c_sample - aborting");
			print_extern_c_fatal("extern_c_sample", any::type_name::<O>(), -99, &detail, None, Some(&bt));
			abort();
		}
	}
}

fn usage_from_sample(sample: Option<OperatorSample>) -> ExternCStateUsage {
	let mut usage = ExternCStateUsage::default();
	if let Some(sample) = sample {
		if let Some(memory) = sample.memory {
			usage.state_entries = memory.entries.as_u64();
			usage.state_bytes = memory.bytes.as_bytes();
		}
		if let Some(rows) = sample.row_number_cache {
			usage.row_number_entries = rows.entries.as_u64();
			usage.row_number_bytes = rows.bytes.as_bytes();
		}
	}
	usage
}

pub fn create_vtable<O: ExternCOperator>() -> ExternCOperatorVTable {
	ExternCOperatorVTable {
		apply: extern_c_apply::<O>,
		on_timer: extern_c_on_timer::<O>,
		destroy: extern_c_destroy::<O>,
		sample: extern_c_sample::<O>,
		lateness_ms: extern_c_lateness_ms::<O>,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::metrics::heap::StateMemory;
	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::*;

	fn memory(entries: u64, bytes: u64) -> StateMemory {
		StateMemory::new(Count::new(entries), ByteSize::from_bytes(bytes))
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
	fn an_operator_reporting_nothing_yields_a_zeroed_usage() {
		let usage = usage_from_sample(None);

		assert_eq!(usage.state_bytes, 0);
		assert_eq!(usage.state_entries, 0);
		assert_eq!(usage.row_number_bytes, 0);
		assert_eq!(usage.row_number_entries, 0);
	}
}
