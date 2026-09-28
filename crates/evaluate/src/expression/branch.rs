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
	open: Vec<bool>,
	described: Vec<String>,
}

impl BranchLayout {
	pub fn new<'a>(columns: impl IntoIterator<Item = (&'a str, ValueType, bool)>) -> Self {
		let mut layout = Self {
			types: Vec::new(),
			open: Vec::new(),
			described: Vec::new(),
		};
		for (name, ty, fits_any) in columns {
			layout.described.push(describe(name, &ty));
			layout.types.push(ty.inner_type().clone());
			layout.open.push(fits_any);
		}
		layout
	}

	pub fn types(&self) -> &[ValueType] {
		&self.types
	}

	pub fn admit<'a>(
		&mut self,
		columns: impl IntoIterator<Item = (&'a str, ValueType, bool)>,
		fragment: &Fragment,
	) -> Result<()> {
		let incoming: Vec<(&str, ValueType, bool)> = columns.into_iter().collect();
		let mut disagrees = incoming.len() != self.types.len();
		if !disagrees {
			for ((slot, open), (_, ty, fits_any)) in
				self.types.iter_mut().zip(self.open.iter_mut()).zip(incoming.iter())
			{
				if *fits_any {
					continue;
				}
				let ty = ty.inner_type();
				if *open {
					*slot = ty.clone();
					*open = false;
				} else if ty != slot {
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
					actual: incoming.iter().map(|(name, ty, _)| describe(name, ty)).collect(),
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
