// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Deref, sync::Arc};

use super::Transform;

#[derive(Clone)]
pub struct Transforms(Arc<TransformsInner>);

impl Transforms {
	pub fn empty() -> Transforms {
		Transforms::builder().build()
	}

	pub fn builder() -> TransformsBuilder {
		TransformsBuilder(TransformsInner {
			transforms: HashMap::new(),
		})
	}
}

impl Deref for Transforms {
	type Target = TransformsInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Clone)]
pub struct TransformsInner {
	transforms: HashMap<String, Arc<dyn Fn() -> Box<dyn Transform> + Send + Sync>>,
}

impl TransformsInner {
	pub fn get_transform(&self, name: &str) -> Option<Box<dyn Transform>> {
		self.transforms.get(name).map(|func| func())
	}

	pub fn has_transform(&self, name: &str) -> bool {
		self.transforms.contains_key(name)
	}
}

pub struct TransformsBuilder(TransformsInner);

impl TransformsBuilder {
	pub fn register<F, T>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> T + Send + Sync + 'static,
		T: Transform + 'static,
	{
		self.0.transforms.insert(name.to_string(), Arc::new(move || Box::new(init()) as Box<dyn Transform>));

		self
	}

	pub fn build(self) -> Transforms {
		Transforms(Arc::new(self.0))
	}
}
