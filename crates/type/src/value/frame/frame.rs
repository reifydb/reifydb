// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{self, Display, Formatter},
	ops::{Deref, Index},
};

use serde::{Deserialize, Serialize};

use super::column::FrameColumn;
use crate::{
	util::unicode::UnicodeWidthStr,
	value::{Value, datetime::DateTime, row_number::RowNumber},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Frame {
	pub row_numbers: Vec<RowNumber>,
	pub created_at: Vec<DateTime>,
	pub updated_at: Vec<DateTime>,
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
			created_at: Vec::new(),
			updated_at: Vec::new(),
			columns,
		}
	}

	pub fn with_row_numbers(columns: Vec<FrameColumn>, row_numbers: Vec<RowNumber>) -> Self {
		Self {
			row_numbers,
			created_at: Vec::new(),
			updated_at: Vec::new(),
			columns,
		}
	}

	pub fn to_rows(&self) -> Vec<Vec<(String, Value)>> {
		let row_count = self.first().map_or(0, |c| c.data.len());
		(0..row_count)
			.map(|row_idx| {
				self.columns.iter().map(|col| (col.name.clone(), col.data.get_value(row_idx))).collect()
			})
			.collect()
	}
}

impl Display for Frame {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let row_count = self.first().map_or(0, |c| c.data.len());
		let has_row_numbers = !self.row_numbers.is_empty();
		let has_created_at = !self.created_at.is_empty();
		let has_updated_at = !self.updated_at.is_empty();

		let mut col_widths: Vec<usize> = Vec::new();

		if has_row_numbers {
			let header_width = "#rownum".width();
			let max_val_width = self.row_numbers.iter().map(|rn| rn.to_string().width()).max().unwrap_or(0);
			col_widths.push(header_width.max(max_val_width));
		}
		if has_created_at {
			let header_width = "#created_at".width();
			let max_val_width = self.created_at.iter().map(|ts| ts.to_string().width()).max().unwrap_or(0);
			col_widths.push(header_width.max(max_val_width));
		}
		if has_updated_at {
			let header_width = "#updated_at".width();
			let max_val_width = self.updated_at.iter().map(|ts| ts.to_string().width()).max().unwrap_or(0);
			col_widths.push(header_width.max(max_val_width));
		}

		for col in &self.columns {
			let header_width = escape_control_chars(&col.name).width();
			let mut max_val_width = 0;
			for i in 0..col.data.len() {
				max_val_width = max_val_width.max(escape_control_chars(&col.data.as_string(i)).width());
			}
			col_widths.push(header_width.max(max_val_width));
		}

		for w in &mut col_widths {
			*w += 2;
		}

		let sep: String = if col_widths.is_empty() {
			"++".to_string()
		} else {
			col_widths.iter().map(|w| format!("+{}", "-".repeat(*w + 2))).collect::<String>() + "+"
		};

		writeln!(f, "{}", sep)?;

		let mut header_parts = Vec::new();
		let mut col_idx = 0;
		if has_row_numbers {
			let name = "#rownum";
			let w = col_widths[col_idx];
			let pad = w - name.width();
			let l = pad / 2;
			let r = pad - l;
			header_parts.push(format!(" {:l$}{}{:r$} ", "", name, ""));
			col_idx += 1;
		}
		if has_created_at {
			let name = "#created_at";
			let w = col_widths[col_idx];
			let pad = w - name.width();
			let l = pad / 2;
			let r = pad - l;
			header_parts.push(format!(" {:l$}{}{:r$} ", "", name, ""));
			col_idx += 1;
		}
		if has_updated_at {
			let name = "#updated_at";
			let w = col_widths[col_idx];
			let pad = w - name.width();
			let l = pad / 2;
			let r = pad - l;
			header_parts.push(format!(" {:l$}{}{:r$} ", "", name, ""));
			col_idx += 1;
		}
		for col in &self.columns {
			let name = escape_control_chars(&col.name);
			let w = col_widths[col_idx];
			let pad = w - name.width();
			let l = pad / 2;
			let r = pad - l;
			header_parts.push(format!(" {:l$}{}{:r$} ", "", name, ""));
			col_idx += 1;
		}
		writeln!(f, "|{}|", header_parts.join("|"))?;
		writeln!(f, "{}", sep)?;

		for row_idx in 0..row_count {
			let mut row_parts = Vec::new();
			let mut col_idx = 0;
			if has_row_numbers {
				let w = col_widths[col_idx];
				let val = self.row_numbers[row_idx].to_string();
				let pad = w - val.width();
				let l = pad / 2;
				let r = pad - l;
				row_parts.push(format!(" {:l$}{}{:r$} ", "", val, ""));
				col_idx += 1;
			}
			if has_created_at {
				let w = col_widths[col_idx];
				let val = self.created_at[row_idx].to_string();
				let pad = w - val.width();
				let l = pad / 2;
				let r = pad - l;
				row_parts.push(format!(" {:l$}{}{:r$} ", "", val, ""));
				col_idx += 1;
			}
			if has_updated_at {
				let w = col_widths[col_idx];
				let val = self.updated_at[row_idx].to_string();
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
