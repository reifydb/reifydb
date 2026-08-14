// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{env, fmt::Write as _, thread};

pub const FATAL_BANNER: &str = "======================== REIFYDB FATAL ========================";
pub const FATAL_FOOTER: &str = "==============================================================";
pub const ISSUE_URL: &str = "https://github.com/reifydb/reifydb/issues";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FatalKind {
	Panic,
	Invariant,
	Error,
}

impl FatalKind {
	pub fn as_str(&self) -> &'static str {
		match self {
			FatalKind::Panic => "panic",
			FatalKind::Invariant => "invariant violated",
			FatalKind::Error => "unexpected error",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
	pub file: String,
	pub line: u32,
	pub column: u32,
}

impl Origin {
	pub fn new(file: impl Into<String>, line: u32, column: u32) -> Self {
		Self {
			file: file.into(),
			line,
			column,
		}
	}

	pub fn error_id(&self) -> String {
		let stem = self.file.rsplit('/').next().unwrap_or(&self.file).trim_end_matches(".rs");
		format!("ERR-{}:{}", stem, self.line)
	}
}

#[derive(Debug, Clone)]
pub struct FatalReport {
	pub kind: FatalKind,
	pub component: Option<String>,
	pub reason: String,
	pub origin: Option<Origin>,
	pub thread_name: String,
	pub backtrace: Option<String>,
	pub context: Vec<(String, String)>,
}

impl FatalReport {
	pub fn new(kind: FatalKind, reason: impl Into<String>) -> Self {
		Self {
			kind,
			component: None,
			reason: reason.into(),
			origin: None,
			thread_name: current_thread_name(),
			backtrace: None,
			context: Vec::new(),
		}
	}

	pub fn component(mut self, component: impl Into<String>) -> Self {
		self.component = Some(component.into());
		self
	}

	pub fn origin(mut self, origin: Origin) -> Self {
		self.origin = Some(origin);
		self
	}

	pub fn backtrace(mut self, backtrace: impl Into<String>) -> Self {
		self.backtrace = Some(backtrace.into());
		self
	}

	pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
		self.context.push((key.into(), value.into()));
		self
	}

	pub fn error_id(&self) -> String {
		self.origin.as_ref().map(Origin::error_id).unwrap_or_else(|| "ERR-unknown".to_string())
	}

	pub fn render(&self) -> String {
		let mut out = String::with_capacity(1024);
		let _ = writeln!(out, "{}", FATAL_BANNER);
		let _ = writeln!(out, "id:        {}", self.error_id());
		let _ = writeln!(out, "kind:      {}", self.kind.as_str());
		if let Some(component) = &self.component {
			let _ = writeln!(out, "component: {}", component);
		}
		let _ = writeln!(out, "reason:    {}", self.reason);
		if let Some(origin) = &self.origin {
			let _ = writeln!(out, "location:  {}:{}:{}", origin.file, origin.line, origin.column);
		}
		let _ = writeln!(out, "thread:    {}", self.thread_name);
		for (key, value) in &self.context {
			let _ = writeln!(out, "{:<10} {}", format!("{}:", key), value);
		}
		let _ = writeln!(out, "version:   {}", env!("CARGO_PKG_VERSION"));
		let _ = writeln!(
			out,
			"build:     {} ({})",
			option_env!("GIT_HASH").unwrap_or("unknown"),
			option_env!("BUILD_DATE").unwrap_or("unknown")
		);
		let _ = writeln!(out, "platform:  {} {}", env::consts::OS, env::consts::ARCH);
		match &self.backtrace {
			Some(backtrace) => {
				let _ = writeln!(out, "backtrace:\n{}", backtrace.trim_end());
			}
			None => {
				let _ = writeln!(out, "backtrace: <unavailable, set RUST_BACKTRACE=1>");
			}
		}
		let _ = writeln!(out, "\nThis is a bug in ReifyDB. Please report it with everything above:");
		let _ = writeln!(out, "{}", ISSUE_URL);
		let _ = write!(out, "{}", FATAL_FOOTER);
		out
	}
}

fn current_thread_name() -> String {
	let current = thread::current();
	match current.name() {
		Some(name) => format!("{} ({:?})", name, current.id()),
		None => format!("<unnamed> ({:?})", current.id()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_error_id_pins_the_source_line_so_two_tickets_from_one_seam_collapse() {
		// Deriving the id from anything unstable (a timestamp, a pointer, the thread) would make every
		// occurrence of one bug look like a separate bug.
		let origin = Origin::new("crates/store-operator/src/commit/buffer.rs", 214, 9);

		assert_eq!(origin.error_id(), "ERR-buffer:214");
		assert_eq!(
			Origin::new("crates/store-operator/src/commit/buffer.rs", 214, 77).error_id(),
			"ERR-buffer:214",
			"the column must not enter the id, or a formatting change would fork the ticket"
		);
	}

	#[test]
	fn a_report_without_an_origin_still_renders_an_id_rather_than_dying() {
		// A panic caught by the hook has no file/line of ours, so the report must degrade and never fail.
		let report = FatalReport::new(FatalKind::Panic, "something impossible");

		assert_eq!(report.error_id(), "ERR-unknown");
		assert!(report.render().contains("id:        ERR-unknown"));
	}

	#[test]
	fn the_render_carries_every_field_a_ticket_needs() {
		// Each field below is one a maintainer cannot reconstruct after the fact; dropping any turns the report
		// into "it crashed".
		let rendered = FatalReport::new(FatalKind::Invariant, "watermark moved backwards: 9 -> 4")
			.component("flow supervisor")
			.origin(Origin::new("crates/flow/src/x.rs", 12, 3))
			.backtrace("0: reifydb_flow::x::apply\n1: reifydb_runtime::pool::run")
			.with("flow", "7")
			.render();

		assert!(rendered.contains(FATAL_BANNER), "the banner is what makes the block greppable in a log");
		assert!(rendered.contains("id:        ERR-x:12"));
		assert!(rendered.contains("kind:      invariant violated"));
		assert!(rendered.contains("component: flow supervisor"));
		assert!(rendered.contains("reason:    watermark moved backwards: 9 -> 4"));
		assert!(rendered.contains("location:  crates/flow/src/x.rs:12:3"));
		assert!(rendered.contains("flow:      7"), "caller context must survive into the report");
		assert!(rendered.contains("0: reifydb_flow::x::apply"));
		assert!(rendered.contains(ISSUE_URL), "a report nobody can file is not a report");
		assert!(rendered.contains(env!("CARGO_PKG_VERSION")));
		assert!(rendered.trim_end().ends_with(FATAL_FOOTER));
	}

	#[test]
	fn a_missing_backtrace_says_how_to_get_one_instead_of_going_silent() {
		// A blank line here would read as "no stack exists" rather than "you did not ask for one".
		let rendered = FatalReport::new(FatalKind::Error, "storage flush failed").render();

		assert!(rendered.contains("RUST_BACKTRACE=1"));
	}

	#[test]
	fn the_thread_is_recorded_because_the_hook_fires_far_from_the_caller() {
		// The bug class this exists for is a background actor dying quietly, and without the thread name the
		// report cannot say which one.
		let rendered = FatalReport::new(FatalKind::Panic, "boom").render();

		assert!(rendered.contains("thread:    "));
	}
}
