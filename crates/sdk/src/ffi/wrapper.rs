// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Wrapper that bridges Rust operators to FFI interface.
//!
//! FFI function return codes:
//! - `< 0`: Unrecoverable error - process will abort immediately
//! - `0`: Success
//! - `> 0`: Recoverable error (reserved for future use)

use std::{
	any::{self, Any},
	backtrace::Backtrace,
	cell::RefCell,
	ffi::c_void,
	io::Write,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
	slice,
	slice::from_raw_parts,
};

use reifydb_abi::{
	context::context::ContextFFI,
	data::column::ColumnsFFI,
	flow::{
		change::ChangeFFI,
		diff::{DiffFFI, DiffType},
	},
	operator::vtable::OperatorVTableFFI,
};
use reifydb_core::interface::change::Change;
use reifydb_type::value::{datetime::DateTime, row_number::RowNumber};
use tracing::{Span, error, instrument, warn};

use crate::{
	ffi::Arena,
	operator::{FFIOperator, context::OperatorContext},
};

thread_local! {
	/// Detail string stored by the innermost error-producing site and consumed
	/// by the abort-printing site. Set whenever an FFI entry point is about to
	/// return a negative code, cleared after the FATAL block is printed.
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

/// Summarize a ChangeFFI without unmarshalling its columns. Safe to call from
/// FATAL paths because it only touches the struct header + diff-type tags.
///
/// # Safety
/// - `input` must be a valid pointer to `ChangeFFI` or null.
unsafe fn describe_change_input(input: *const ChangeFFI) -> String {
	if input.is_null() {
		return "<null>".to_string();
	}
	let ffi = unsafe { &*input };
	let types = if !ffi.diffs.is_null() && ffi.diff_count > 0 {
		let diffs: &[DiffFFI] = unsafe { from_raw_parts(ffi.diffs, ffi.diff_count) };
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

/// Emit the unified FATAL diagnostic block to stderr and flush.
///
/// `input_description` is optional because `ffi_pull` has no ChangeFFI input
/// and `ffi_destroy` has no input at all.
fn print_ffi_fatal(
	entry: &str,
	operator: &str,
	code: i32,
	detail: &str,
	input_description: Option<&str>,
	backtrace: Option<&Backtrace>,
) {
	let mut err = std::io::stderr().lock();
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

/// Wrapper that adapts a Rust operator to the FFI interface
pub struct OperatorWrapper<O: FFIOperator> {
	operator: O,
	arena: RefCell<Arena>,
}

impl<O: FFIOperator> OperatorWrapper<O> {
	/// Create a new operator wrapper
	pub fn new(operator: O) -> Self {
		Self {
			operator,
			arena: RefCell::new(Arena::new()),
		}
	}

	/// Get a pointer to this wrapper as c_void
	pub fn as_ptr(&mut self) -> *mut c_void {
		self as *mut _ as *mut c_void
	}

	/// Create from a raw pointer
	pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
		unsafe { &mut *(ptr as *mut Self) }
	}
}

/// Unmarshal FFI input to Change
#[inline]
#[instrument(name = "unmarshal", level = "trace", skip_all)]
fn unmarshal_input(arena: &mut Arena, input: *const ChangeFFI) -> Result<Change, i32> {
	unsafe {
		match arena.unmarshal_change(&*input) {
			Ok(change) => Ok(change),
			Err(e) => {
				warn!(?e, "Unmarshal failed");
				set_fatal_detail(e);
				Err(-3)
			}
		}
	}
}

/// Apply the operator
#[inline]
#[instrument(name = "operator_apply", level = "trace", skip_all)]
fn apply_operator<O: FFIOperator>(operator: &mut O, ctx: *mut ContextFFI, input_change: Change) -> Result<Change, i32> {
	let mut op_ctx = OperatorContext::new(ctx);
	match operator.apply(&mut op_ctx, input_change) {
		Ok(change) => Ok(change),
		Err(e) => {
			warn!(?e, "Apply failed");
			set_fatal_detail(format!("{:?}", e));
			Err(-2)
		}
	}
}

/// Marshal Change to FFI output
#[inline]
#[instrument(name = "marshal", level = "trace", skip_all)]
fn marshal_output(arena: &mut Arena, output_change: &Change, output: *mut ChangeFFI) {
	unsafe {
		*output = arena.marshal_change(output_change);
	}
}

/// # Safety
///
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>` created by `Box::new`.
/// - `ctx` must be a valid pointer to a `ContextFFI`.
/// - `input` must be a valid pointer to a `ChangeFFI` for reading.
/// - `output` must be a valid pointer to a `ChangeFFI` for writing.
#[instrument(name = "flow::operator::ffi::apply", level = "debug", skip_all, fields(
	operator_type = any::type_name::<O>(),
	input_diffs,
	output_diffs
))]
pub unsafe extern "C" fn ffi_apply<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	input: *const ChangeFFI,
	output: *mut ChangeFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);

		let mut arena = wrapper.arena.borrow_mut();
		arena.clear();

		// Unmarshal input
		let input_change = match unmarshal_input(&mut arena, input) {
			Ok(change) => {
				Span::current().record("input_diffs", change.diffs.len());
				change
			}
			Err(code) => return code,
		};

		// Apply operator
		let output_change = match apply_operator::<O>(&mut wrapper.operator, ctx, input_change) {
			Ok(change) => {
				Span::current().record("output_diffs", change.diffs.len());
				change
			}
			Err(code) => return code,
		};

		// Marshal output
		marshal_output(&mut arena, &output_change, output);

		0 // Success
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
/// - `row_numbers` must be valid for reading `count` elements, or null if `count` is 0.
/// - `output` must be a valid pointer to a `ColumnsFFI` for writing.
#[instrument(name = "flow::operator::ffi::pull", level = "debug", skip_all, fields(
	operator_type = any::type_name::<O>(),
	row_count = count,
	rows_returned
))]
pub unsafe extern "C" fn ffi_pull<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	row_numbers: *const u64,
	count: usize,
	output: *mut ColumnsFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		unsafe {
			let wrapper = OperatorWrapper::<O>::from_ptr(instance);

			let mut arena = wrapper.arena.borrow_mut();
			arena.clear();

			// Convert row numbers
			let numbers: Vec<RowNumber> = if !row_numbers.is_null() && count > 0 {
				slice::from_raw_parts(row_numbers, count).iter().map(|&n| RowNumber::from(n)).collect()
			} else {
				Vec::new()
			};

			// Create context
			let mut op_ctx = OperatorContext::new(ctx);

			// Call the operator
			let columns = match wrapper.operator.pull(&mut op_ctx, &numbers) {
				Ok(cols) => {
					Span::current().record("rows_returned", cols.row_count());
					cols
				}
				Err(e) => {
					warn!(?e, "pull failed");
					set_fatal_detail(format!("{:?}", e));
					return -2;
				}
			};

			*output = arena.marshal_columns(&columns);

			0 // Success
		}
	}));

	let (code, backtrace) = match result {
		Ok(code) => (code, None),
		Err(payload) => {
			let bt = Backtrace::force_capture();
			set_fatal_detail(describe_panic_payload(&payload));
			error!("Panic in ffi_pull");
			(-99, Some(bt))
		}
	};

	if code < 0 {
		let detail = take_fatal_detail().unwrap_or_default();
		let input_desc = format!("row_count={}", count);
		print_ffi_fatal(
			"ffi_pull",
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
/// - `output` must be a valid pointer to a `ChangeFFI` for writing.
#[instrument(name = "flow::operator::ffi::tick", level = "debug", skip_all, fields(
	operator_type = any::type_name::<O>(),
	output_diffs
))]
pub unsafe extern "C" fn ffi_tick<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	timestamp_nanos: u64,
	output: *mut ChangeFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);

		let mut arena = wrapper.arena.borrow_mut();
		arena.clear();

		let timestamp = DateTime::from_nanos(timestamp_nanos);
		let mut op_ctx = OperatorContext::new(ctx);

		match wrapper.operator.tick(&mut op_ctx, timestamp) {
			Ok(Some(change)) => {
				Span::current().record("output_diffs", change.diffs.len());
				marshal_output(&mut arena, &change, output);
				0 // Success with output
			}
			Ok(None) => {
				1 // Success without output (no-op)
			}
			Err(e) => {
				warn!(?e, "Tick failed");
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
			error!("Panic in ffi_tick");
			(-99, Some(bt))
		}
	};

	if code < 0 {
		let detail = take_fatal_detail().unwrap_or_default();
		let input_desc = format!("timestamp_nanos={}", timestamp_nanos);
		print_ffi_fatal(
			"ffi_tick",
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
/// - `instance` must be a valid pointer to an `OperatorWrapper<O>` originally created by `Box::new`, or null (in which
///   case this is a no-op).
pub unsafe extern "C" fn ffi_destroy<O: FFIOperator>(instance: *mut c_void) {
	if instance.is_null() {
		return;
	}

	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		// Reconstruct the Box from the raw pointer and let it drop
		let _wrapper = Box::from_raw(instance as *mut OperatorWrapper<O>);
		// Wrapper will be dropped here, cleaning up the operator
	}));

	if let Err(payload) = result {
		let bt = Backtrace::force_capture();
		let detail = describe_panic_payload(&payload);
		error!("Panic in ffi_destroy - aborting");
		print_ffi_fatal("ffi_destroy", any::type_name::<O>(), -99, &detail, None, Some(&bt));
		abort();
	}
}

/// Create the vtable for an operator type
pub fn create_vtable<O: FFIOperator>() -> OperatorVTableFFI {
	OperatorVTableFFI {
		apply: ffi_apply::<O>,
		pull: ffi_pull::<O>,
		tick: ffi_tick::<O>,
		destroy: ffi_destroy::<O>,
	}
}
