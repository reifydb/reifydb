// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::Debug;

use reifydb_type::{Blob, Date, DateTime, Duration, Time};

use crate::value::column::ColumnData;

mod decimal;
mod i128;
mod i16;
mod i32;
mod i64;
mod i8;
mod int;
mod u128;
mod u16;
mod u32;
mod u64;
mod u8;
mod uint;
mod undefined;
mod uuid;
mod value;

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

impl Push<&str> for ColumnData {
	fn push(&mut self, value: &str) {
		self.push(value.to_string());
	}
}
