// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::Write;

use super::Diagnostic;
use crate::fragment::OwnedFragment;

pub trait DiagnosticRenderer {
	fn render(&self, diagnostic: &Diagnostic) -> String;
}

pub struct DefaultRenderer;

pub fn get_line(source: &str, line: u32) -> &str {
	source.lines().nth((line - 1) as usize).unwrap_or("")
}

impl DiagnosticRenderer for DefaultRenderer {
	fn render(&self, diagnostic: &Diagnostic) -> String {
		let mut output = String::new();

		if !diagnostic.cause.is_some() {
			self.render_flat(&mut output, diagnostic);
		} else {
			self.render_nested(&mut output, diagnostic, 0);
		}

		output
	}
}

impl DefaultRenderer {
	fn render_flat(&self, output: &mut String, diagnostic: &Diagnostic) {
		let _ = writeln!(output, "Error {}", diagnostic.code);
		let _ = writeln!(output, "  {}", diagnostic.message);
		let _ = writeln!(output);

		if let OwnedFragment::Statement {
			line,
			column,
			text,
			..
		} = &diagnostic.fragment
		{
			let fragment = text;
			let line = line.0;
			let col = column.0;
			let statement = diagnostic.statement.as_ref().map(|x| x.as_str()).unwrap_or("");

			let _ = writeln!(output, "LOCATION");
			let _ = writeln!(output, "  line {}, column {}", line, col);
			let _ = writeln!(output);

			let line_content = get_line(statement, line);

			let _ = writeln!(output, "CODE");
			let _ = writeln!(output, "  {} │ {}", line, line_content);
			let fragment_start = line_content.find(fragment.as_str()).unwrap_or(col as usize);
			let _ = writeln!(output, "    │ {}{}", " ".repeat(fragment_start), "~".repeat(fragment.len()));
			let _ = writeln!(output, "    │");

			let label_text = diagnostic.label.as_deref().unwrap_or("");
			let fragment_center = fragment_start + fragment.len() / 2;
			let label_center_offset = if label_text.len() / 2 > fragment_center {
				0
			} else {
				fragment_center - label_text.len() / 2
			};

			let _ = writeln!(output, "    │ {}{}", " ".repeat(label_center_offset), label_text);
			let _ = writeln!(output);
		}

		if let Some(help) = &diagnostic.help {
			let _ = writeln!(output, "HELP");
			let _ = writeln!(output, "  {}", help);
			let _ = writeln!(output);
		}

		if let Some(col) = &diagnostic.column {
			let _ = writeln!(output, "COLUMN");
			let _ = writeln!(output, "  column `{}` is of type `{}`", col.name, col.r#type);
			let _ = writeln!(output);
		}

		if !diagnostic.notes.is_empty() {
			let _ = writeln!(output, "NOTES");
			for note in &diagnostic.notes {
				let _ = writeln!(output, "  • {}", note);
			}
		}
	}

	fn render_nested(&self, output: &mut String, diagnostic: &Diagnostic, depth: usize) {
		let indent = if depth == 0 {
			""
		} else {
			"  "
		};
		let prefix = if depth == 0 {
			""
		} else {
			"↳ "
		};

		// Main error line
		let _ = writeln!(output, "{}{} Error {}: {}", indent, prefix, diagnostic.code, diagnostic.message);

		// Location info
		if let OwnedFragment::Statement {
			line,
			column,
			text,
			..
		} = &diagnostic.fragment
		{
			let fragment = text;
			let line = line.0;
			let col = column.0;
			let statement = diagnostic.statement.as_ref().map(|x| x.as_str()).unwrap_or("");

			let _ = writeln!(
				output,
				"{}  at {} (line {}, column {})",
				indent,
				if statement.is_empty() {
					"unknown".to_string()
				} else {
					format!("\"{}\"", fragment)
				},
				line,
				col
			);
			let _ = writeln!(output);

			// Code visualization
			let line_content = get_line(statement, line);

			let _ = writeln!(output, "{}  {} │ {}", indent, line, line_content);
			let fragment_start = line_content.find(fragment.as_str()).unwrap_or(col as usize);
			let _ = writeln!(
				output,
				"{}    │ {}{}",
				indent,
				" ".repeat(fragment_start),
				"~".repeat(fragment.len())
			);

			let label_text = diagnostic.label.as_deref().unwrap_or("");
			if !label_text.is_empty() {
				let fragment_center = fragment_start + fragment.len() / 2;
				let label_center_offset = if label_text.len() / 2 > fragment_center {
					0
				} else {
					fragment_center - label_text.len() / 2
				};

				let _ = writeln!(
					output,
					"{}    │ {}{}",
					indent,
					" ".repeat(label_center_offset),
					label_text
				);
			}
			let _ = writeln!(output);
		}

		// Handle nested cause first (if exists)
		if let Some(cause) = &diagnostic.cause {
			self.render_nested(output, cause, depth + 1);
		}

		// Help section
		if let Some(help) = &diagnostic.help {
			let _ = writeln!(output, "{}  help: {}", indent, help);
		}

		// Column info
		if let Some(col) = &diagnostic.column {
			let _ = writeln!(output, "{}  column `{}` is of type `{}`", indent, col.name, col.r#type);
		}

		// Notes
		if !diagnostic.notes.is_empty() {
			for note in &diagnostic.notes {
				let _ = writeln!(output, "{}  note: {}", indent, note);
			}
		}

		// Add spacing between diagnostic levels
		if depth > 0 {
			let _ = writeln!(output);
		}
	}
}

impl DefaultRenderer {
	pub fn render_string(diagnostic: &Diagnostic) -> String {
		DefaultRenderer.render(diagnostic)
	}
}
