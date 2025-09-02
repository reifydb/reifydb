// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Uuid4, Uuid7};

use crate::value::columnar::{data::ColumnData, push::Push};

impl Push<Uuid4> for ColumnData {
	fn push(&mut self, value: Uuid4) {
		match self {
			ColumnData::Uuid4(container) => container.push(value),
			ColumnData::Undefined(container) => {
				let mut new_container =
					ColumnData::uuid4(vec![
						Uuid4::default();
						container.len()
					]);
				if let ColumnData::Uuid4(new_container) =
					&mut new_container
				{
					new_container.push(value);
				}
				*self = new_container;
			}
			other => {
				panic!(
					"called `push::<Uuid4>()` on incompatible EngineColumnData::{:?}",
					other.get_type()
				);
			}
		}
	}
}

impl Push<Uuid7> for ColumnData {
	fn push(&mut self, value: Uuid7) {
		match self {
			ColumnData::Uuid7(container) => container.push(value),
			ColumnData::Undefined(container) => {
				let mut new_container =
					ColumnData::uuid7(vec![
						Uuid7::default();
						container.len()
					]);
				if let ColumnData::Uuid7(new_container) =
					&mut new_container
				{
					new_container.push(value);
				}
				*self = new_container;
			}
			other => {
				panic!(
					"called `push::<Uuid7>()` on incompatible EngineColumnData::{:?}",
					other.get_type()
				);
			}
		}
	}
}
