// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::Debug;

use reifydb_type::value::{
	blob::Blob, date::Date, datetime::DateTime, dictionary::DictionaryEntryId, duration::Duration, time::Time,
};

use crate::value::column::ColumnData;

pub mod decimal;
pub mod i128;
pub mod i16;
pub mod i32;
pub mod i64;
pub mod i8;
pub mod int;
pub mod u128;
pub mod u16;
pub mod u32;
pub mod u64;
pub mod u8;
pub mod uint;
pub mod undefined;
pub mod uuid;
pub mod value;

pub trait Push<T> {
	fn push(&mut self, value: T);
}

impl ColumnData {
	pub fn push<T>(&mut self, value: T)
	where
		Self: Push<T>,
		T: Debug,
	{
		<Self as Push<T>>::push(self, value)
	}
}

macro_rules! impl_push {
	($t:ty, $variant:ident, $factory:ident) => {
		impl Push<$t> for ColumnData {
			fn push(&mut self, value: $t) {
				match self {
					ColumnData::$variant(container) => {
						container.push(value);
					}
					ColumnData::Undefined(container) => {
						let mut new_container =
							ColumnData::$factory(vec![<$t>::default(); container.len()]);
						if let ColumnData::$variant(new_container) = &mut new_container {
							new_container.push(value);
						}
						*self = new_container;
					}
					other => panic!(
						"called `push::<{}>()` on EngineColumnData::{:?}",
						stringify!($t),
						other.get_type()
					),
				}
			}
		}
	};
}

impl Push<bool> for ColumnData {
	fn push(&mut self, value: bool) {
		match self {
			ColumnData::Bool(container) => {
				container.push(value);
			}
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::bool(vec![false; container.len()]);
				if let ColumnData::Bool(new_container) = &mut new_container {
					new_container.push(value);
				}
				*self = new_container;
			}
			other => panic!("called `push::<bool>()` on EngineColumnData::{:?}", other.get_type()),
		}
	}
}

impl_push!(f32, Float4, float4);
impl_push!(f64, Float8, float8);
impl_push!(Date, Date, date);
impl_push!(DateTime, DateTime, datetime);
impl_push!(Time, Time, time);
impl_push!(Duration, Duration, duration);

impl Push<Blob> for ColumnData {
	fn push(&mut self, value: Blob) {
		match self {
			ColumnData::Blob {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::blob(vec![Blob::default(); container.len()]);
				if let ColumnData::Blob {
					container: new_container,
					..
				} = &mut new_container
				{
					new_container.push(value);
				}
				*self = new_container;
			}
			other => panic!("called `push::<Blob>()` on EngineColumnData::{:?}", other.get_type()),
		}
	}
}

impl Push<String> for ColumnData {
	fn push(&mut self, value: String) {
		match self {
			ColumnData::Utf8 {
				container,
				..
			} => {
				container.push(value);
			}
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::utf8(vec![String::default(); container.len()]);
				if let ColumnData::Utf8 {
					container: new_container,
					..
				} = &mut new_container
				{
					new_container.push(value);
				}
				*self = new_container;
			}
			other => {
				panic!("called `push::<String>()` on EngineColumnData::{:?}", other.get_type())
			}
		}
	}
}

impl Push<DictionaryEntryId> for ColumnData {
	fn push(&mut self, value: DictionaryEntryId) {
		match self {
			ColumnData::DictionaryId(container) => {
				container.push(value);
			}
			ColumnData::Undefined(container) => {
				let mut new_container =
					ColumnData::dictionary_id(vec![DictionaryEntryId::default(); container.len()]);
				if let ColumnData::DictionaryId(new_container) = &mut new_container {
					new_container.push(value);
				}
				*self = new_container;
			}
			other => panic!(
				"called `push::<DictionaryEntryId>()` on EngineColumnData::{:?}",
				other.get_type()
			),
		}
	}
}

impl Push<&str> for ColumnData {
	fn push(&mut self, value: &str) {
		self.push(value.to_string());
	}
}
