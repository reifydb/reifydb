// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt::{self, Write};

use reifydb_type::{
	util::unicode::UnicodeWidthStr,
	value::frame::{column::FrameColumn, frame::Frame},
};

/// Frame renderer with various rendering options
pub struct FrameRenderer;

impl FrameRenderer {
	/// Render the frame with all features enabled (including encoded numbers if present)
	pub fn render_full(frame: &Frame) -> Result<String, fmt::Error> {
		let mut output = String::new();
		Self::render_full_to(frame, &mut output)?;
		Ok(output)
	}

	/// Render the frame without encoded numbers
	pub fn render_without_row_numbers(frame: &Frame) -> Result<String, fmt::Error> {
		let mut output = String::new();
		Self::render_without_row_numbers_to(frame, &mut output)?;
		Ok(output)
	}

	/// Render the frame with all features to the given formatter
	pub fn render_full_to(frame: &Frame, f: &mut dyn Write) -> fmt::Result {
		Self::render_internal(frame, f, true)
	}

	/// Render the frame without encoded numbers to the given formatter
	pub fn render_without_row_numbers_to(frame: &Frame, f: &mut dyn Write) -> fmt::Result {
		Self::render_internal(frame, f, false)
	}

	/// Internal rendering implementation
	fn render_internal(frame: &Frame, f: &mut dyn Write, include_row_numbers: bool) -> fmt::Result {
		let row_count = frame.first().map_or(0, |c| c.data.len());
		let has_row_numbers = include_row_numbers && !frame.row_numbers.is_empty();
		let has_created_at = !frame.created_at.is_empty();
		let has_updated_at = !frame.updated_at.is_empty();
		let col_count = frame.len()
			+ if has_row_numbers {
				1
			} else {
				0
			} + if has_created_at {
			1
		} else {
			0
		} + if has_updated_at {
			1
		} else {
			0
		};

		// Get the display order for regular columns
		let column_order = Self::get_column_display_order(frame);

		let mut col_widths = vec![0; col_count];

		// Calculate widths for system columns
		let mut sys_col_idx = 0;
		if has_row_numbers {
			col_widths[sys_col_idx] = Self::display_width("#rownum");
			for row_num in &frame.row_numbers {
				col_widths[sys_col_idx] =
					col_widths[sys_col_idx].max(Self::display_width(&row_num.to_string()));
			}
			sys_col_idx += 1;
		}
		if has_created_at {
			col_widths[sys_col_idx] = Self::display_width("#created_at");
			for ts in &frame.created_at {
				col_widths[sys_col_idx] =
					col_widths[sys_col_idx].max(Self::display_width(&ts.to_string()));
			}
			sys_col_idx += 1;
		}
		if has_updated_at {
			col_widths[sys_col_idx] = Self::display_width("#updated_at");
			for ts in &frame.updated_at {
				col_widths[sys_col_idx] =
					col_widths[sys_col_idx].max(Self::display_width(&ts.to_string()));
			}
			sys_col_idx += 1;
		}
		let row_num_col_idx = sys_col_idx;

		for (display_idx, &col_idx) in column_order.iter().enumerate() {
			let col = &frame[col_idx];
			let display_name = Self::escape_control_chars(&col.name);
			col_widths[row_num_col_idx + display_idx] = Self::display_width(&display_name);
		}

		for row_numberx in 0..row_count {
			for (display_idx, &col_idx) in column_order.iter().enumerate() {
				let col = &frame[col_idx];
				let s = Self::extract_string_value(col, row_numberx);
				col_widths[row_num_col_idx + display_idx] =
					col_widths[row_num_col_idx + display_idx].max(Self::display_width(&s));
			}
		}

		// Add padding
		for w in &mut col_widths {
			*w += 2;
		}

		let sep = format!("+{}+", col_widths.iter().map(|w| "-".repeat(*w + 2)).collect::<Vec<_>>().join("+"));
		writeln!(f, "{}", sep)?;

		let mut header = Vec::new();

		// Add system column headers
		let mut sys_idx = 0;
		if has_row_numbers {
			let w = col_widths[sys_idx];
			let name = "#rownum";
			let pad = w - Self::display_width(name);
			let l = pad / 2;
			let r = pad - l;
			header.push(format!(" {:left$}{}{:right$} ", "", name, "", left = l, right = r));
			sys_idx += 1;
		}
		if has_created_at {
			let w = col_widths[sys_idx];
			let name = "#created_at";
			let pad = w - Self::display_width(name);
			let l = pad / 2;
			let r = pad - l;
			header.push(format!(" {:left$}{}{:right$} ", "", name, "", left = l, right = r));
			sys_idx += 1;
		}
		if has_updated_at {
			let w = col_widths[sys_idx];
			let name = "#updated_at";
			let pad = w - Self::display_width(name);
			let l = pad / 2;
			let r = pad - l;
			header.push(format!(" {:left$}{}{:right$} ", "", name, "", left = l, right = r));
			sys_idx += 1;
		}
		let _ = sys_idx;

		// Add regular column headers
		for (display_idx, &col_idx) in column_order.iter().enumerate() {
			let col = &frame[col_idx];
			let w = col_widths[row_num_col_idx + display_idx];
			let name = Self::escape_control_chars(&col.name);
			let pad = w - Self::display_width(&name);
			let l = pad / 2;
			let r = pad - l;
			header.push(format!(" {:left$}{}{:right$} ", "", name, "", left = l, right = r));
		}

		writeln!(f, "|{}|", header.join("|"))?;

		writeln!(f, "{}", sep)?;

		for row_numberx in 0..row_count {
			let mut row = Vec::new();

			// Add system column values
			let mut sys_idx = 0;
			if has_row_numbers {
				let w = col_widths[sys_idx];
				let s = frame.row_numbers[row_numberx].to_string();
				let pad = w - Self::display_width(&s);
				let l = pad / 2;
				let r = pad - l;
				row.push(format!(" {:left$}{}{:right$} ", "", s, "", left = l, right = r));
				sys_idx += 1;
			}
			if has_created_at {
				let w = col_widths[sys_idx];
				let s = frame.created_at[row_numberx].to_string();
				let pad = w - Self::display_width(&s);
				let l = pad / 2;
				let r = pad - l;
				row.push(format!(" {:left$}{}{:right$} ", "", s, "", left = l, right = r));
				sys_idx += 1;
			}
			if has_updated_at {
				let w = col_widths[sys_idx];
				let s = frame.updated_at[row_numberx].to_string();
				let pad = w - Self::display_width(&s);
				let l = pad / 2;
				let r = pad - l;
				row.push(format!(" {:left$}{}{:right$} ", "", s, "", left = l, right = r));
				sys_idx += 1;
			}
			let _ = sys_idx;

			// Add regular column values
			for (display_idx, &col_idx) in column_order.iter().enumerate() {
				let col = &frame[col_idx];
				let w = col_widths[row_num_col_idx + display_idx];
				let s = Self::extract_string_value(col, row_numberx);
				let pad = w - Self::display_width(&s);
				let l = pad / 2;
				let r = pad - l;
				row.push(format!(" {:left$}{}{:right$} ", "", s, "", left = l, right = r));
			}

			writeln!(f, "|{}|", row.join("|"))?;
		}

		writeln!(f, "{}", sep)
	}

	/// Calculate the display width of a string, handling newlines properly.
	/// For strings with newlines, returns the width of the longest line.
	/// For strings without newlines, returns the unicode display width.
	fn display_width(s: &str) -> usize {
		if s.contains('\n') {
			s.lines().map(|line| line.width()).max().unwrap_or(0)
		} else {
			s.width()
		}
	}

	/// Escape newlines and tabs in a string for single-line display.
	/// Replaces '\n' with "\\n" and '\t' with "\\t".
	fn escape_control_chars(s: &str) -> String {
		s.replace('\n', "\\n").replace('\t', "\\t")
	}

	/// Create a column display order (no special handling needed since encoded numbers are separate)
	fn get_column_display_order(frame: &Frame) -> Vec<usize> {
		(0..frame.len()).collect()
	}

	/// Extract string value from column at given encoded index, with proper escaping
	fn extract_string_value(col: &FrameColumn, row_numberx: usize) -> String {
		let s = col.data.as_string(row_numberx);
		Self::escape_control_chars(&s)
	}
}
