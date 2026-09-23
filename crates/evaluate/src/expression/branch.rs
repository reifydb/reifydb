// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::view::group_by::common_key_type;
use reifydb_value::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::value_type::ValueType,
};

use crate::Result;

pub struct BranchLayout {
	types: Vec<ValueType>,
	described: Vec<String>,
}

impl BranchLayout {
	pub fn new<'a>(columns: impl IntoIterator<Item = (&'a str, ValueType)>) -> Self {
		let (types, described) =
			columns.into_iter().map(|(name, ty)| (ty.inner_type().clone(), describe(name, &ty))).unzip();
		Self {
			types,
			described,
		}
	}

	pub fn types(&self) -> &[ValueType] {
		&self.types
	}

	pub fn admit<'a>(
		&mut self,
		columns: impl IntoIterator<Item = (&'a str, ValueType)>,
		fragment: &Fragment,
	) -> Result<()> {
		let incoming: Vec<(&str, ValueType)> = columns.into_iter().collect();
		let mut disagrees = incoming.len() != self.types.len();
		if !disagrees {
			for (slot, (_, ty)) in self.types.iter_mut().zip(incoming.iter()) {
				let ty = ty.inner_type();
				if *slot == ValueType::Any {
					*slot = ty.clone();
				} else if *ty != ValueType::Any && ty != slot {
					match common_key_type(slot, ty) {
						Some(common) => *slot = common,
						None => {
							disagrees = true;
							break;
						}
					}
				}
			}
		}

		if disagrees {
			return Err(TypeError::Runtime {
				kind: RuntimeErrorKind::ConditionalBranchMismatch {
					expected: self.described.clone(),
					actual: incoming.iter().map(|(name, ty)| describe(name, ty)).collect(),
					fragment: fragment.clone(),
				},
				message: "conditional branches produce different columns".to_string(),
			}
			.into());
		}
		Ok(())
	}
}

fn describe(name: &str, ty: &ValueType) -> String {
	format!("{}: {}", name, ty)
}
