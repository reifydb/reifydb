// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Display, Formatter},
	ops::{Deref, Index},
};

use serde::{Deserialize, Serialize};

use super::column::FrameColumn;
use crate::{
	util::unicode::UnicodeWidthStr,
	value::{
		Value,
		datetime::DateTime,
		row_number::RowNumber,
		system_columns::{SystemColumn, SystemColumns},
	},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Frame {
	pub system: SystemColumns,
	pub columns: Vec<FrameColumn>,
}

impl Frame {
	#[inline]
	pub fn row_numbers(&self) -> &[RowNumber] {
		self.system.row_numbers()
	}

	#[inline]
	pub fn created_at(&self) -> &[DateTime] {
		self.system.created_at()
	}

	#[inline]
	pub fn updated_at(&self) -> &[DateTime] {
		self.system.updated_at()
	}

	#[inline]
	pub fn time(&self) -> &[DateTime] {
		self.system.time()
	}
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

fn present_system_columns(frame: &Frame) -> Vec<(&'static str, Vec<String>)> {
	let candidates = [
		(
			SystemColumn::RowNumbers.name(),
			frame.row_numbers().iter().map(|v| v.to_string()).collect::<Vec<_>>(),
		),
		(SystemColumn::CreatedAt.name(), frame.created_at().iter().map(|v| v.to_string()).collect()),
		(SystemColumn::UpdatedAt.name(), frame.updated_at().iter().map(|v| v.to_string()).collect()),
	];
	candidates.into_iter().filter(|(_, cells)| !cells.is_empty()).collect()
}

fn centered(width: usize, content: &str) -> String {
	let pad = width - content.width();
	let l = pad / 2;
	let r = pad - l;
	format!(" {:l$}{}{:r$} ", "", content, "")
}

impl Frame {
	pub fn new(columns: Vec<FrameColumn>) -> Self {
		Self {
			system: SystemColumns::empty(),
			columns,
		}
	}

	pub fn with_row_numbers(columns: Vec<FrameColumn>, row_numbers: Vec<RowNumber>) -> Self {
		Self {
			system: SystemColumns::new(row_numbers, Vec::new(), Vec::new(), Vec::new(), Vec::new()),
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
		let system = present_system_columns(self);

		let mut col_widths: Vec<usize> = Vec::new();

		for (header, cells) in &system {
			let max_val_width = cells.iter().map(|c| c.width()).max().unwrap_or(0);
			col_widths.push(header.width().max(max_val_width));
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
		for (col_idx, (header, _)) in system.iter().enumerate() {
			header_parts.push(centered(col_widths[col_idx], header));
		}
		for (offset, col) in self.columns.iter().enumerate() {
			let name = escape_control_chars(&col.name);
			header_parts.push(centered(col_widths[system.len() + offset], &name));
		}
		writeln!(f, "|{}|", header_parts.join("|"))?;
		writeln!(f, "{}", sep)?;

		for row_idx in 0..row_count {
			let mut row_parts = Vec::new();
			for (col_idx, (_, cells)) in system.iter().enumerate() {
				row_parts.push(centered(col_widths[col_idx], &cells[row_idx]));
			}
			for (offset, col) in self.columns.iter().enumerate() {
				let val = escape_control_chars(&col.data.as_string(row_idx));
				row_parts.push(centered(col_widths[system.len() + offset], &val));
			}
			writeln!(f, "|{}|", row_parts.join("|"))?;
		}

		writeln!(f, "{}", sep)
	}
}
