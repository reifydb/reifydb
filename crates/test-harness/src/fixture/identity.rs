// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_auth::method::{github::GithubProvider, password::PasswordProvider, solana::SolanaProvider};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{auth::AuthenticationProvider, catalog::identity::Identity};
use reifydb_runtime::context::{clock::Clock, rng::Rng};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::{Value, identity::IdentityId};

use crate::engine::AsEngine;

pub fn identity(name: &str) -> IdentityBuilder {
	IdentityBuilder {
		name: name.to_string(),
		attributes: Vec::new(),
		authentications: Vec::new(),
	}
}

struct AttributeSpec {
	name: String,
	value: Value,
}

struct AuthenticationSpec {
	method: String,
	config: HashMap<String, String>,
	lookup: Option<(String, String)>,
}

pub struct IdentityBuilder {
	name: String,
	attributes: Vec<AttributeSpec>,
	authentications: Vec<AuthenticationSpec>,
}

impl IdentityBuilder {
	pub fn attribute(mut self, name: &str, value: Value) -> Self {
		self.attributes.push(AttributeSpec {
			name: name.to_string(),
			value,
		});
		self
	}

	pub fn solana_key(mut self, public_key: &str) -> Self {
		self.authentications.push(AuthenticationSpec {
			method: "solana".to_string(),
			config: HashMap::from([("public_key".to_string(), public_key.to_string())]),
			lookup: Some(("solana_public_key".to_string(), public_key.to_string())),
		});
		self
	}

	pub fn github_user(mut self, id: u64, login: &str) -> Self {
		self.authentications.push(AuthenticationSpec {
			method: "github".to_string(),
			config: HashMap::from([
				("user_id".to_string(), id.to_string()),
				("login".to_string(), login.to_string()),
			]),
			lookup: Some(("github_user_id".to_string(), id.to_string())),
		});
		self
	}

	pub fn password(mut self, password: &str) -> Self {
		self.authentications.push(AuthenticationSpec {
			method: "password".to_string(),
			config: HashMap::from([("password".to_string(), password.to_string())]),
			lookup: None,
		});
		self
	}

	pub fn create(self, engine: &impl AsEngine) -> Identity {
		let engine = engine.standard_engine();
		let catalog = engine.catalog();
		let clock = Clock::Real;
		let rng = Rng::seeded(42);

		let mut admin = engine.begin_admin(IdentityId::root()).unwrap();
		let identity = catalog.create_identity(&mut admin, &self.name, &clock, &rng).unwrap();

		for attribute in &self.attributes {
			set_attribute(&catalog, &mut admin, identity.id, &attribute.name, attribute.value.clone());
		}

		for authentication in &self.authentications {
			let properties =
				provider(&authentication.method, &clock).create(&rng, &authentication.config).unwrap();
			catalog.create_authentication(&mut admin, identity.id, &authentication.method, properties)
				.unwrap();
			if let Some((name, value)) = &authentication.lookup {
				set_attribute(&catalog, &mut admin, identity.id, name, Value::Utf8(value.clone()));
			}
		}

		admin.commit().unwrap();
		identity
	}
}

fn provider(method: &str, clock: &Clock) -> Box<dyn AuthenticationProvider> {
	match method {
		"solana" => Box::new(SolanaProvider::new(clock.clone())),
		"github" => Box::new(GithubProvider),
		"password" => Box::new(PasswordProvider),
		other => panic!("identity builder has no provider for method '{other}'"),
	}
}

fn set_attribute(catalog: &Catalog, admin: &mut AdminTransaction, identity: IdentityId, name: &str, value: Value) {
	let attribute =
		match catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut *admin), name).unwrap() {
			Some(attribute) => attribute,
			None => catalog.create_identity_attribute(admin, name, value.get_type()).unwrap(),
		};
	catalog.set_identity_attribute_value(admin, identity, &attribute, value).unwrap();
}
