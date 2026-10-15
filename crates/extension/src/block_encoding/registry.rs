// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Deref, sync::Arc};

use super::BlockEncoding;

#[derive(Clone)]
pub struct BlockEncodings(Arc<BlockEncodingsInner>);

impl BlockEncodings {
	pub fn empty() -> BlockEncodings {
		BlockEncodings::builder().configure()
	}

	pub fn builder() -> BlockEncodingsConfigurator {
		BlockEncodingsConfigurator(BlockEncodingsInner {
			encodings: HashMap::new(),
		})
	}
}

impl Deref for BlockEncodings {
	type Target = BlockEncodingsInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Clone)]
pub struct BlockEncodingsInner {
	encodings: HashMap<String, Arc<dyn Fn() -> Box<dyn BlockEncoding> + Send + Sync>>,
}

impl BlockEncodingsInner {
	pub fn get(&self, name: &str) -> Option<Box<dyn BlockEncoding>> {
		self.encodings.get(name).map(|init| init())
	}
}

pub struct BlockEncodingsConfigurator(BlockEncodingsInner);

impl BlockEncodingsConfigurator {
	pub fn register<F, E>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> E + Send + Sync + 'static,
		E: BlockEncoding + 'static,
	{
		if self.0.encodings.contains_key(name) {
			panic!("block encoding '{name}' registered twice");
		}
		self.0.encodings.insert(name.to_string(), Arc::new(move || Box::new(init()) as Box<dyn BlockEncoding>));
		self
	}

	pub fn configure(self) -> BlockEncodings {
		BlockEncodings(Arc::new(self.0))
	}
}

#[cfg(test)]
mod tests {
	use arrow_schema::FieldRef;
	use reifydb_value::{Result, value::Value};

	use super::BlockEncodings;
	use crate::block_encoding::{BlockEncoding, BoundBlockEncoding};

	struct Fixed(u32);

	impl BlockEncoding for Fixed {
		fn version(&self) -> u32 {
			self.0
		}

		fn bind(&self, _user: &[FieldRef], _config: &[(String, Value)]) -> Result<Box<dyn BoundBlockEncoding>> {
			unreachable!("registry tests never bind")
		}
	}

	#[test]
	fn an_unregistered_name_is_not_found() {
		// A hit for an unknown name would let CREATE accept an encoding nobody registered.
		assert!(BlockEncodings::empty().get("delta").is_none());
		let registry = BlockEncodings::builder().register("a", || Fixed(1)).configure();
		assert!(registry.get("b").is_none());
	}

	#[test]
	fn each_name_resolves_to_its_own_encoding() {
		// A lookup that mixes names decodes a block with an encoding that never wrote it.
		let registry =
			BlockEncodings::builder().register("a", || Fixed(1)).register("b", || Fixed(2)).configure();
		assert_eq!(registry.get("a").unwrap().version(), 1);
		assert_eq!(registry.get("b").unwrap().version(), 2);
	}

	#[test]
	fn clones_share_one_registry() {
		// Engine and sub-store hold clones; a clone that loses entries splits decode from encode.
		let registry = BlockEncodings::builder().register("a", || Fixed(1)).configure();
		let clone = registry.clone();
		assert_eq!(clone.get("a").unwrap().version(), 1);
	}

	#[test]
	#[should_panic(expected = "block encoding 'a' registered twice")]
	fn a_second_register_under_one_name_panics() {
		// A silent replace lets a different encoding decode blocks the first one wrote.
		let _ = BlockEncodings::builder().register("a", || Fixed(1)).register("a", || Fixed(2));
	}
}
