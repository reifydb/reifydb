// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Deref, sync::Arc};

use super::Procedure;

#[derive(Clone)]
pub struct Procedures(Arc<ProceduresInner>);

impl Procedures {
	pub fn empty() -> Procedures {
		Procedures::builder().build()
	}

	pub fn builder() -> ProceduresBuilder {
		ProceduresBuilder(ProceduresInner {
			procedures: HashMap::new(),
		})
	}
}

impl Deref for Procedures {
	type Target = ProceduresInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Clone)]
pub struct ProceduresInner {
	procedures: HashMap<String, Arc<dyn Fn() -> Box<dyn Procedure> + Send + Sync>>,
}

impl ProceduresInner {
	pub fn get_procedure(&self, name: &str) -> Option<Box<dyn Procedure>> {
		self.procedures.get(name).map(|func| func())
	}

	pub fn has_procedure(&self, name: &str) -> bool {
		self.procedures.contains_key(name)
	}
}

pub struct ProceduresBuilder(ProceduresInner);

impl ProceduresBuilder {
	pub fn register<F, P>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> P + Send + Sync + 'static,
		P: Procedure + 'static,
	{
		self.0.procedures.insert(name.to_string(), Arc::new(move || Box::new(init()) as Box<dyn Procedure>));

		self
	}

	pub fn build(self) -> Procedures {
		Procedures(Arc::new(self.0))
	}
}
