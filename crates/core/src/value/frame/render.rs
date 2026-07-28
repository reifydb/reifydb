// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Write};

use reifydb_value::{
	reifydb_assertions,
	util::unicode::UnicodeWidthStr,
	value::{
		frame::{column::FrameColumn, frame::Frame},
		system_columns::SystemColumn,
	},
};

pub struct FrameRenderer;

struct SystemCells {
	header: &'static str,
	cells: Vec<String>,
}

fn system_cells(frame: &Frame, include_row_numbers: bool) -> Vec<SystemCells> {
	let mut present = Vec::new();

	let mut push = |header: &'static str, cells: Vec<String>| {
		if !cells.is_empty() {
			present.push(SystemCells {
				header,
				cells,
			});
		}
	};

	if include_row_numbers {
		push(SystemColumn::RowNumbers.name(), frame.row_numbers().iter().map(|v| v.to_string()).collect());
	}
	push(SystemColumn::CreatedAt.name(), frame.created_at().iter().map(|v| v.to_string()).collect());
	push(SystemColumn::UpdatedAt.name(), frame.updated_at().iter().map(|v| v.to_string()).collect());
	push(SystemColumn::Time.name(), frame.time().iter().map(|v| v.to_string()).collect());

	present
}

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
		let system = system_cells(frame, include_row_numbers);
		let column_order = Self::get_column_display_order(frame);

		let col_widths = Self::compute_column_widths(frame, &column_order, row_count, &system);
		let sep = Self::separator_line(&col_widths);

		writeln!(f, "{}", sep)?;
		Self::emit_header(frame, f, &column_order, &col_widths, &system)?;
		writeln!(f, "{}", sep)?;
		Self::emit_data_rows(frame, f, &column_order, &col_widths, row_count, &system)?;
		writeln!(f, "{}", sep)
	}

	#[inline]
	fn compute_column_widths(
		frame: &Frame,
		column_order: &[usize],
		row_count: usize,
		system: &[SystemCells],
	) -> Vec<usize> {
		let mut col_widths = vec![0; system.len() + frame.len()];

		for (sys_idx, column) in system.iter().enumerate() {
			col_widths[sys_idx] = Self::display_width(column.header);
			for cell in &column.cells {
				col_widths[sys_idx] = col_widths[sys_idx].max(Self::display_width(cell));
			}
		}
		let row_num_col_idx = system.len();

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
	fn emit_header(
		frame: &Frame,
		f: &mut dyn Write,
		column_order: &[usize],
		col_widths: &[usize],
		system: &[SystemCells],
	) -> fmt::Result {
		let mut header = Vec::new();

		for (sys_idx, column) in system.iter().enumerate() {
			header.push(Self::format_cell(col_widths[sys_idx], column.header));
		}
		let row_num_col_idx = system.len();

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
	fn emit_data_rows(
		frame: &Frame,
		f: &mut dyn Write,
		column_order: &[usize],
		col_widths: &[usize],
		row_count: usize,
		system: &[SystemCells],
	) -> fmt::Result {
		for row_numberx in 0..row_count {
			let mut row = Vec::new();

			for (sys_idx, column) in system.iter().enumerate() {
				row.push(Self::format_cell(col_widths[sys_idx], &column.cells[row_numberx]));
			}
			let row_num_col_idx = system.len();

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

#[cfg(test)]
mod tests {
	use reifydb_value::value::{
		container::number::NumberContainer,
		datetime::DateTime,
		frame::{column::FrameColumn, data::FrameColumnData},
		row_number::RowNumber,
		system_columns::SystemColumns,
	};

	use super::*;

	fn dt(nanos: u64) -> DateTime {
		DateTime::from_nanos(nanos)
	}

	fn frame(system: SystemColumns) -> Frame {
		Frame {
			system,
			columns: vec![FrameColumn {
				name: "n".to_string(),
				data: FrameColumnData::Int4(NumberContainer::new(vec![1, 2])),
			}],
		}
	}

	fn headers(rendered: &str) -> Vec<String> {
		rendered
			.lines()
			.nth(1)
			.expect("a rendered frame has a header line")
			.split('|')
			.map(|c| c.trim().to_string())
			.filter(|c| !c.is_empty())
			.collect()
	}

	#[test]
	// Intent: #time is shown alongside the other sidecars. It is the one column a user cannot reconstruct from what
	// is on screen - #rownum and the wall stamps are the engine's own bookkeeping, but #time is the instant every
	// windowing and retention decision is made against, and an author who declared `time: event` has to be able to
	// see where their rows actually landed. Rendering the wall stamps while hiding #time is the specific gap this
	// pins: it makes a backfill that silently re-dated to arrival look identical to one that worked.
	// Mutation: drop the Time push from system_cells and #time vanishes while #created_at survives, which is exactly
	// the asymmetry that made a dropped #time invisible.
	fn time_is_rendered_beside_the_other_system_columns() {
		let rendered = FrameRenderer::render_full(&frame(SystemColumns::new(
			vec![RowNumber(1), RowNumber(2)],
			Vec::new(),
			vec![dt(10), dt(20)],
			vec![dt(30), dt(40)],
			vec![dt(50), dt(60)],
		)))
		.unwrap();

		assert_eq!(headers(&rendered), vec!["#rownum", "#created_at", "#updated_at", "#time", "n"]);
		assert!(rendered.contains(&dt(50).to_string()), "the first row's #time must appear in the body");
		assert!(rendered.contains(&dt(60).to_string()), "the second row's #time must appear in the body");
	}

	#[test]
	// Intent: an absent sidecar renders no column at all, and every other one keeps its place. Sidecars are
	// independently optional - a frame assembled mid-pipeline may carry #time and nothing else - so the header must
	// be derived from what is actually present rather than from a fixed layout that would emit a blank or
	// epoch-valued column for whatever is missing.
	// Mutation: emit a fixed four-column system header and the second case below grows three columns of 1970.
	fn only_the_sidecars_the_frame_carries_are_rendered() {
		let no_time = FrameRenderer::render_full(&frame(SystemColumns::new(
			vec![RowNumber(1), RowNumber(2)],
			Vec::new(),
			vec![dt(10), dt(20)],
			vec![dt(30), dt(40)],
			Vec::new(),
		)))
		.unwrap();
		assert_eq!(headers(&no_time), vec!["#rownum", "#created_at", "#updated_at", "n"]);

		let only_time = FrameRenderer::render_full(&frame(SystemColumns::new(
			Vec::new(),
			Vec::new(),
			Vec::new(),
			Vec::new(),
			vec![dt(50), dt(60)],
		)))
		.unwrap();
		assert_eq!(headers(&only_time), vec!["#time", "n"]);
	}

	#[test]
	// Intent: render_without_row_numbers suppresses #rownum and nothing else. #rownum is an identifier the caller
	// may not want to show; #time is data about the row, and dropping it along with the identifier would hide the
	// instant in exactly the presentation contexts where a reader has the least other context to reconstruct it.
	// Mutation: gate the whole system block on include_row_numbers and #time disappears here too.
	fn suppressing_row_numbers_keeps_the_timestamps() {
		let rendered = FrameRenderer::render_without_row_numbers(&frame(SystemColumns::new(
			vec![RowNumber(1), RowNumber(2)],
			Vec::new(),
			vec![dt(10), dt(20)],
			vec![dt(30), dt(40)],
			vec![dt(50), dt(60)],
		)))
		.unwrap();

		assert_eq!(headers(&rendered), vec!["#created_at", "#updated_at", "#time", "n"]);
	}

	#[test]
	// Intent: the separator, header and every data row agree on the column count, whatever mix of sidecars is
	// present. The widths are computed once and indexed from three places, so a sidecar counted in one and not the
	// others produces a table that is misaligned or panics mid-render - and #time is the first column to arrive
	// since those three call sites were written.
	// Mutation: count only the wall stamps when sizing col_widths and the rows here stop matching the separator.
	fn every_line_agrees_on_the_column_count() {
		for system in [
			SystemColumns::new(vec![RowNumber(1), RowNumber(2)], Vec::new(), Vec::new(), Vec::new(), vec![
				dt(50),
				dt(60),
			]),
			SystemColumns::new(
				vec![RowNumber(1), RowNumber(2)],
				Vec::new(),
				vec![dt(10), dt(20)],
				vec![dt(30), dt(40)],
				vec![dt(50), dt(60)],
			),
			SystemColumns::empty(),
		] {
			let rendered = FrameRenderer::render_full(&frame(system)).unwrap();
			let lines: Vec<&str> = rendered.lines().collect();
			let width = lines[0].len();
			for (i, line) in lines.iter().enumerate() {
				assert_eq!(line.len(), width, "line {i} has a different width:\n{rendered}");
			}
		}
	}
}
