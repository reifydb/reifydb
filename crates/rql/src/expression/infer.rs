// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::expression::Expression;
use reifydb_type::{NamedTypeConstraint, Type};

pub fn infer_type(expr: &Expression, named_types: &[impl NamedTypeConstraint]) -> Option<Type> {
	todo!()
}
