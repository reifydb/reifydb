// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

use arrow_array::RecordBatch;

use crate::{
	util::unicode::UnicodeWidthStr,
	value::{Value, column_view::ColumnView, diff_type::DiffType, system_columns::SystemColumn},
};

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
	pub batch: RecordBatch,
	pub op: Option<DiffType>,
}

impl From<RecordBatch> for Frame {
	fn from(batch: RecordBatch) -> Self {
		Self {
			batch,
			op: None,
		}
	}
}

fn escape_control_chars(s: &str) -> String {
	s.replace('\n', "\\n").replace('\t', "\\t")
}

fn centered(width: usize, content: &str) -> String {
	let pad = width - content.width();
	let l = pad / 2;
	let r = pad - l;
	format!(" {:l$}{}{:r$} ", "", content, "")
}

impl Frame {
	pub fn with_op(mut self, op: DiffType) -> Self {
		self.op = Some(op);
		self
	}

	pub fn to_rows(&self) -> Vec<Vec<(String, Value)>> {
		let views = self.views().expect("a frame column does not match its field");
		(0..self.batch.num_rows())
			.map(|row_idx| {
				views.iter().map(|(name, view)| (name.clone(), view.get_value(row_idx))).collect()
			})
			.collect()
	}

	fn views(&self) -> crate::Result<Vec<(String, ColumnView<'_>)>> {
		let schema = self.batch.schema_ref();
		self.batch
			.columns()
			.iter()
			.zip(schema.fields().iter())
			.map(|(array, field)| {
				Ok((field.name().clone(), ColumnView::try_from((array, field.as_ref()))?))
			})
			.collect()
	}
}

impl Display for Frame {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let row_count = self.batch.num_rows();
		let mut columns = self.views().map_err(|_| fmt::Error)?;
		if let Some(index) = columns.iter().position(|(name, _)| name == SystemColumn::RowNumbers.name()) {
			let rownum = columns.remove(index);
			columns.insert(0, rownum);
		}

		let mut col_widths: Vec<usize> = Vec::new();

		for (name, view) in &columns {
			let header_width = escape_control_chars(name).width();
			let mut max_val_width = 0;
			for i in 0..view.len() {
				max_val_width = max_val_width.max(escape_control_chars(&view.as_string(i)).width());
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
		for (col_idx, (name, _)) in columns.iter().enumerate() {
			header_parts.push(centered(col_widths[col_idx], &escape_control_chars(name)));
		}
		writeln!(f, "|{}|", header_parts.join("|"))?;
		writeln!(f, "{}", sep)?;

		for row_idx in 0..row_count {
			let mut row_parts = Vec::new();
			for (col_idx, (_, view)) in columns.iter().enumerate() {
				let val = escape_control_chars(&view.as_string(row_idx));
				row_parts.push(centered(col_widths[col_idx], &val));
			}
			writeln!(f, "|{}|", row_parts.join("|"))?;
		}

		writeln!(f, "{}", sep)
	}
}
