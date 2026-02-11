// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::fragment::Fragment;

use super::context::ExecContext;

pub struct CompiledExpr {
	inner: CompiledExprInner,
	access_column_name: Option<String>,
}

enum CompiledExprInner {
	Single(Box<dyn Fn(&ExecContext) -> crate::Result<Column> + Send + Sync>),
	Multi(Box<dyn Fn(&ExecContext) -> crate::Result<Vec<Column>> + Send + Sync>),
}

impl CompiledExpr {
	pub fn new(f: impl Fn(&ExecContext) -> crate::Result<Column> + Send + Sync + 'static) -> Self {
		Self {
			inner: CompiledExprInner::Single(Box::new(f)),
			access_column_name: None,
		}
	}

	pub fn new_multi(f: impl Fn(&ExecContext) -> crate::Result<Vec<Column>> + Send + Sync + 'static) -> Self {
		Self {
			inner: CompiledExprInner::Multi(Box::new(f)),
			access_column_name: None,
		}
	}

	pub fn new_access(
		name: String,
		f: impl Fn(&ExecContext) -> crate::Result<Column> + Send + Sync + 'static,
	) -> Self {
		Self {
			inner: CompiledExprInner::Single(Box::new(f)),
			access_column_name: Some(name),
		}
	}

	pub fn access_column_name(&self) -> Option<&str> {
		self.access_column_name.as_deref()
	}

	pub fn execute(&self, ctx: &ExecContext) -> crate::Result<Column> {
		match &self.inner {
			CompiledExprInner::Single(f) => f(ctx),
			CompiledExprInner::Multi(f) => {
				let columns = f(ctx)?;
				Ok(columns.into_iter().next().unwrap_or_else(|| Column {
					name: Fragment::internal("undefined"),
					data: ColumnData::with_capacity(
						reifydb_type::value::r#type::Type::Undefined,
						0,
					),
				}))
			}
		}
	}

	pub fn execute_multi(&self, ctx: &ExecContext) -> crate::Result<Vec<Column>> {
		match &self.inner {
			CompiledExprInner::Single(f) => Ok(vec![f(ctx)?]),
			CompiledExprInner::Multi(f) => f(ctx),
		}
	}
}
