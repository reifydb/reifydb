// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Write};

use reifydb_value::{
	reifydb_assertions,
	util::unicode::UnicodeWidthStr,
	value::frame::{column::FrameColumn, frame::Frame},
};

pub struct FrameRenderer;

impl FrameRenderer {
	pub fn render_full(frame: &Frame) -> Result<String, fmt::Error> {
		let mut output = String::new();
		Self::render_full_to(frame, &mut output)?;
		Ok(output)
	}

	pub fn render_without_row_numbers(frame: &Frame) -> Result<String, fmt::Error> {
		let mut output = String::new();
		Self::render_without_row_numbers_to(frame, &mut output)?;
		Ok(output)
	}

	pub fn render_full_to(frame: &Frame, f: &mut dyn Write) -> fmt::Result {
		Self::render_internal(frame, f, true)
	}

	pub fn render_without_row_numbers_to(frame: &Frame, f: &mut dyn Write) -> fmt::Result {
		Self::render_internal(frame, f, false)
	}

	fn render_internal(frame: &Frame, f: &mut dyn Write, include_row_numbers: bool) -> fmt::Result {
		let row_count = frame.first().map_or(0, |c| c.data.len());
		let has_row_numbers = include_row_numbers && !frame.row_numbers.is_empty();
		let has_created_at = !frame.created_at.is_empty();
		let has_updated_at = !frame.updated_at.is_empty();
		let column_order = Self::get_column_display_order(frame);

		let col_widths = Self::compute_column_widths(
			frame,
			&column_order,
			row_count,
			has_row_numbers,
			has_created_at,
			has_updated_at,
		);
		let sep = Self::separator_line(&col_widths);

		writeln!(f, "{}", sep)?;
		Self::emit_header(
			frame,
			f,
			&column_order,
			&col_widths,
			has_row_numbers,
			has_created_at,
			has_updated_at,
		)?;
		writeln!(f, "{}", sep)?;
		Self::emit_data_rows(
			frame,
			f,
			&column_order,
			&col_widths,
			row_count,
			has_row_numbers,
			has_created_at,
			has_updated_at,
		)?;
		writeln!(f, "{}", sep)
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn compute_column_widths(
		frame: &Frame,
		column_order: &[usize],
		row_count: usize,
		has_row_numbers: bool,
		has_created_at: bool,
		has_updated_at: bool,
	) -> Vec<usize> {
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

		let mut col_widths = vec![0; col_count];

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

		for w in &mut col_widths {
			*w += 2;
		}

		col_widths
	}

	#[inline]
	fn separator_line(col_widths: &[usize]) -> String {
		format!("+{}+", col_widths.iter().map(|w| "-".repeat(*w + 2)).collect::<Vec<_>>().join("+"))
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn emit_header(
		frame: &Frame,
		f: &mut dyn Write,
		column_order: &[usize],
		col_widths: &[usize],
		has_row_numbers: bool,
		has_created_at: bool,
		has_updated_at: bool,
	) -> fmt::Result {
		let mut header = Vec::new();

		let mut sys_idx = 0;
		if has_row_numbers {
			header.push(Self::format_cell(col_widths[sys_idx], "#rownum"));
			sys_idx += 1;
		}
		if has_created_at {
			header.push(Self::format_cell(col_widths[sys_idx], "#created_at"));
			sys_idx += 1;
		}
		if has_updated_at {
			header.push(Self::format_cell(col_widths[sys_idx], "#updated_at"));
			sys_idx += 1;
		}
		let row_num_col_idx = sys_idx;

		reifydb_assertions! {
			let needed = row_num_col_idx + column_order.len();
			assert!(
				col_widths.len() >= needed,
				"header system-column count diverged from compute_column_widths, so a data \
				 column would index col_widths out of bounds and panic mid-render \
				 (system cols={row_num_col_idx}, data cols={}, col_widths.len()={})",
				column_order.len(),
				col_widths.len()
			);
		}

		for (display_idx, &col_idx) in column_order.iter().enumerate() {
			let col = &frame[col_idx];
			let name = Self::escape_control_chars(&col.name);
			header.push(Self::format_cell(col_widths[row_num_col_idx + display_idx], &name));
		}

		writeln!(f, "|{}|", header.join("|"))
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn emit_data_rows(
		frame: &Frame,
		f: &mut dyn Write,
		column_order: &[usize],
		col_widths: &[usize],
		row_count: usize,
		has_row_numbers: bool,
		has_created_at: bool,
		has_updated_at: bool,
	) -> fmt::Result {
		for row_numberx in 0..row_count {
			let mut row = Vec::new();

			let mut sys_idx = 0;
			if has_row_numbers {
				let s = frame.row_numbers[row_numberx].to_string();
				row.push(Self::format_cell(col_widths[sys_idx], &s));
				sys_idx += 1;
			}
			if has_created_at {
				let s = frame.created_at[row_numberx].to_string();
				row.push(Self::format_cell(col_widths[sys_idx], &s));
				sys_idx += 1;
			}
			if has_updated_at {
				let s = frame.updated_at[row_numberx].to_string();
				row.push(Self::format_cell(col_widths[sys_idx], &s));
				sys_idx += 1;
			}
			let row_num_col_idx = sys_idx;

			reifydb_assertions! {
				let needed = row_num_col_idx + column_order.len();
				assert!(
					col_widths.len() >= needed,
					"data-row system-column count diverged from compute_column_widths, so a data \
					 column would index col_widths out of bounds and panic mid-render \
					 (system cols={row_num_col_idx}, data cols={}, col_widths.len()={})",
					column_order.len(),
					col_widths.len()
				);
			}

			for (display_idx, &col_idx) in column_order.iter().enumerate() {
				let col = &frame[col_idx];
				let s = Self::extract_string_value(col, row_numberx);
				row.push(Self::format_cell(col_widths[row_num_col_idx + display_idx], &s));
			}

			writeln!(f, "|{}|", row.join("|"))?;
		}

		Ok(())
	}

	fn format_cell(width: usize, content: &str) -> String {
		let pad = width - Self::display_width(content);
		let l = pad / 2;
		let r = pad - l;
		format!(" {:left$}{}{:right$} ", "", content, "", left = l, right = r)
	}

	fn display_width(s: &str) -> usize {
		if s.contains('\n') {
			s.lines().map(|line| line.width()).max().unwrap_or(0)
		} else {
			s.width()
		}
	}

	fn escape_control_chars(s: &str) -> String {
		s.replace('\n', "\\n").replace('\t', "\\t")
	}

	fn get_column_display_order(frame: &Frame) -> Vec<usize> {
		(0..frame.len()).collect()
	}

	fn extract_string_value(col: &FrameColumn, row_numberx: usize) -> String {
		let s = col.data.as_string(row_numberx);
		Self::escape_control_chars(&s)
	}
}
