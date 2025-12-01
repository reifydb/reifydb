// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	fmt::{self, Display, Formatter},
	ops::{Deref, Index},
};

use serde::{Deserialize, Serialize};

use super::FrameColumn;
use crate::{RowNumber, util::unicode::UnicodeWidthStr};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
	pub row_numbers: Vec<RowNumber>,
	pub columns: Vec<FrameColumn>,
}

impl Deref for Frame {
	type Target = [FrameColumn];

	fn deref(&self) -> &Self::Target {
		&self.columns
	}
}

impl Index<usize> for Frame {
	type Output = FrameColumn;

	fn index(&self, index: usize) -> &Self::Output {
		self.columns.index(index)
	}
}

fn escape_control_chars(s: &str) -> String {
	s.replace('\n', "\\n").replace('\t', "\\t")
}

impl Frame {
	pub fn new(columns: Vec<FrameColumn>) -> Self {
		Self {
			row_numbers: Vec::new(),
			columns,
		}
	}

	pub fn with_row_numbers(columns: Vec<FrameColumn>, row_numbers: Vec<RowNumber>) -> Self {
		Self {
			row_numbers,
			columns,
		}
	}
}

impl Display for Frame {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let row_count = self.first().map_or(0, |c| c.data.len());
		let has_row_numbers = !self.row_numbers.is_empty();

		// Calculate column widths
		let mut col_widths: Vec<usize> = Vec::new();

		// Row number column width
		if has_row_numbers {
			let header_width = "rownum".width();
			let max_val_width = self.row_numbers.iter().map(|rn| rn.to_string().width()).max().unwrap_or(0);
			col_widths.push(header_width.max(max_val_width));
		}

		// Regular column widths
		for col in &self.columns {
			let header_width = escape_control_chars(&col.qualified_name()).width();
			let mut max_val_width = 0;
			for i in 0..col.data.len() {
				max_val_width = max_val_width.max(escape_control_chars(&col.data.as_string(i)).width());
			}
			col_widths.push(header_width.max(max_val_width));
		}

		// Add padding
		for w in &mut col_widths {
			*w += 2;
		}

		// Build separator
		let sep: String = if col_widths.is_empty() {
			"++".to_string()
		} else {
			col_widths.iter().map(|w| format!("+{}", "-".repeat(*w + 2))).collect::<String>() + "+"
		};

		writeln!(f, "{}", sep)?;

		// Build header
		let mut header_parts = Vec::new();
		let mut col_idx = 0;
		if has_row_numbers {
			let name = "rownum";
			let w = col_widths[col_idx];
			let pad = w - name.width();
			let l = pad / 2;
			let r = pad - l;
			header_parts.push(format!(" {:l$}{}{:r$} ", "", name, ""));
			col_idx += 1;
		}
		for col in &self.columns {
			let name = escape_control_chars(&col.qualified_name());
			let w = col_widths[col_idx];
			let pad = w - name.width();
			let l = pad / 2;
			let r = pad - l;
			header_parts.push(format!(" {:l$}{}{:r$} ", "", name, ""));
			col_idx += 1;
		}
		writeln!(f, "|{}|", header_parts.join("|"))?;
		writeln!(f, "{}", sep)?;

		// Build rows
		for row_idx in 0..row_count {
			let mut row_parts = Vec::new();
			let mut col_idx = 0;
			if has_row_numbers {
				let w = col_widths[col_idx];
				let val = if row_idx < self.row_numbers.len() {
					self.row_numbers[row_idx].to_string()
				} else {
					"Undefined".to_string()
				};
				let pad = w - val.width();
				let l = pad / 2;
				let r = pad - l;
				row_parts.push(format!(" {:l$}{}{:r$} ", "", val, ""));
				col_idx += 1;
			}
			for col in &self.columns {
				let w = col_widths[col_idx];
				let val = escape_control_chars(&col.data.as_string(row_idx));
				let pad = w - val.width();
				let l = pad / 2;
				let r = pad - l;
				row_parts.push(format!(" {:l$}{}{:r$} ", "", val, ""));
				col_idx += 1;
			}
			writeln!(f, "|{}|", row_parts.join("|"))?;
		}

		writeln!(f, "{}", sep)
	}
}
