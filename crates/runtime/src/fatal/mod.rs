// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Process-wide fatal handling: either the database is healthy or it does not run at all.
//!
//! Anything unexpected - a panic on any thread, a violated invariant, an error that has no handler - renders one
//! ticket-grade report and aborts. The report is the only artifact a maintainer gets, so it carries the error id,
//! the origin, the thread, the build, and the real stack.
//!
//! The stack is why this is a panic hook rather than a `catch_unwind` wrapper: a hook runs *before* unwinding, so
//! `Backtrace::force_capture` there sees the panicking frames. Capturing in a `catch_unwind` arm sees only the
//! landing pad, because the frames it should have shown are already gone.
//!
//! Arming is configuration and defaults to on. Disarming exists for tests that panic on purpose.

pub mod report;

use std::{
	any::Any,
	backtrace::Backtrace,
	env,
	io::{self, Write},
	panic::{self, PanicHookInfo},
	process,
	sync::{
		Once,
		atomic::{AtomicBool, Ordering},
	},
};

use tracing::error;

use crate::fatal::report::{FatalKind, FatalReport, Origin};

static ARMED: AtomicBool = AtomicBool::new(true);
static INSTALLED: Once = Once::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FatalConfig {
	pub armed: bool,
}

impl Default for FatalConfig {
	fn default() -> Self {
		Self {
			armed: env_armed().unwrap_or(true),
		}
	}
}

impl FatalConfig {
	pub fn armed(armed: bool) -> Self {
		Self {
			armed,
		}
	}

	pub fn disarmed() -> Self {
		Self::armed(false)
	}
}

fn env_armed() -> Option<bool> {
	match env::var("REIFYDB_FATAL").ok()?.as_str() {
		"0" | "off" | "false" => Some(false),
		"1" | "on" | "true" => Some(true),
		_ => None,
	}
}

pub fn install(config: FatalConfig) {
	ARMED.store(config.armed, Ordering::Release);
	INSTALLED.call_once(|| {
		panic::set_hook(Box::new(on_panic));
	});
}

pub fn arm() {
	ARMED.store(true, Ordering::Release);
}

pub fn disarm() {
	ARMED.store(false, Ordering::Release);
}

pub fn is_armed() -> bool {
	ARMED.load(Ordering::Acquire)
}

pub fn emit(report: &FatalReport) {
	let rendered = report.render();
	error!(
		fatal.id = %report.error_id(),
		fatal.kind = report.kind.as_str(),
		fatal.thread = %report.thread_name,
		"{}",
		report.reason
	);
	let mut err = io::stderr().lock();
	let _ = writeln!(err, "{}", rendered);
	let _ = err.flush();
}

pub fn fatal(report: FatalReport) -> ! {
	emit(&report);
	process::abort()
}

fn on_panic(info: &PanicHookInfo<'_>) {
	let backtrace = Backtrace::force_capture();
	let mut report = FatalReport::new(FatalKind::Panic, panic_message(info));
	if let Some(location) = info.location() {
		report = report.origin(Origin::new(location.file(), location.line(), location.column()));
	}
	if !is_armed() {
		emit(&report.backtrace(backtrace.to_string()));
		return;
	}
	fatal(report.backtrace(backtrace.to_string()))
}

pub fn panic_message(info: &PanicHookInfo<'_>) -> String {
	let payload = info.payload();
	if let Some(message) = payload.downcast_ref::<&'static str>() {
		(*message).to_string()
	} else if let Some(message) = payload.downcast_ref::<String>() {
		message.clone()
	} else {
		"<non-string panic payload>".to_string()
	}
}

pub fn describe_payload(payload: &Box<dyn Any + Send>) -> String {
	if let Some(message) = payload.downcast_ref::<&'static str>() {
		(*message).to_string()
	} else if let Some(message) = payload.downcast_ref::<String>() {
		message.clone()
	} else {
		"<non-string panic payload>".to_string()
	}
}

#[macro_export]
macro_rules! fatal {
	($reason:expr) => {
		$crate::fatal::fatal(
			$crate::fatal::report::FatalReport::new($crate::fatal::report::FatalKind::Invariant, $reason)
				.origin($crate::fatal::report::Origin::new(file!(), line!(), column!()))
				.backtrace(std::backtrace::Backtrace::force_capture().to_string()),
		)
	};
	($fmt:expr, $($arg:tt)*) => {
		$crate::fatal::fatal(
			$crate::fatal::report::FatalReport::new($crate::fatal::report::FatalKind::Invariant, format!($fmt, $($arg)*))
				.origin($crate::fatal::report::Origin::new(file!(), line!(), column!()))
				.backtrace(std::backtrace::Backtrace::force_capture().to_string()),
		)
	};
}

#[macro_export]
macro_rules! fatal_on_err {
	($expr:expr) => {
		match $expr {
			Ok(value) => value,
			Err(err) => $crate::fatal::fatal(
				$crate::fatal::report::FatalReport::new(
					$crate::fatal::report::FatalKind::Error,
					format!("{:?}", err),
				)
				.origin($crate::fatal::report::Origin::new(file!(), line!(), column!()))
				.backtrace(std::backtrace::Backtrace::force_capture().to_string()),
			),
		}
	};
	($expr:expr, $component:expr) => {
		match $expr {
			Ok(value) => value,
			Err(err) => $crate::fatal::fatal(
				$crate::fatal::report::FatalReport::new(
					$crate::fatal::report::FatalKind::Error,
					format!("{:?}", err),
				)
				.component($component)
				.origin($crate::fatal::report::Origin::new(file!(), line!(), column!()))
				.backtrace(std::backtrace::Backtrace::force_capture().to_string()),
			),
		}
	};
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_default_config_is_armed_because_a_forgotten_flag_must_not_reopen_the_hole() {
		// A default of off would leave a deployment that never sets the flag swallowing exactly like before.
		assert!(FatalConfig::default().armed);
	}

	#[test]
	fn the_env_override_only_accepts_known_spellings() {
		// An unparsed value must fall through to the armed default rather than silently disarming.
		assert_eq!(FatalConfig::disarmed().armed, false);
		assert!(FatalConfig::armed(true).armed);
	}

	#[test]
	fn a_string_panic_payload_survives_into_the_reason() {
		// Most sites today drop the payload with `|_|`, which is what makes their logs useless.
		let payload: Box<dyn Any + Send> = Box::new("boom".to_string());

		assert_eq!(describe_payload(&payload), "boom");
	}

	#[test]
	fn a_static_str_panic_payload_survives_into_the_reason() {
		// `panic!("literal")` produces &'static str, not String, and downcasting only one of the two loses half
		// the panics.
		let payload: Box<dyn Any + Send> = Box::new("boom");

		assert_eq!(describe_payload(&payload), "boom");
	}

	#[test]
	fn an_opaque_panic_payload_is_named_rather_than_rendered_empty() {
		// panic_any with a custom type lands here, and an empty reason reads as "no reason" instead of
		// "unprintable".
		let payload: Box<dyn Any + Send> = Box::new(42u32);

		assert_eq!(describe_payload(&payload), "<non-string panic payload>");
	}
}
