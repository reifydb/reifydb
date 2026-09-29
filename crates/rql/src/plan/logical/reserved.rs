// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{error::diagnostic::query::system_column_reserved, expression::Expression};
use reifydb_value::{fragment::Fragment, return_error};

use crate::Result;

pub(crate) fn reject_reserved_column_name(name: &Fragment) -> Result<()> {
	if name.text().starts_with('#') {
		return_error!(system_column_reserved(name.clone()));
	}
	Ok(())
}

pub(crate) fn reject_reserved_output_names(expressions: &[Expression]) -> Result<()> {
	for expression in expressions {
		if let Expression::Alias(alias) = expression {
			reject_reserved_column_name(&alias.alias.0)?;
		}
	}
	Ok(())
}
