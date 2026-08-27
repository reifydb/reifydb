// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Clock, Error, IdentityId,
	auth::method::password::PasswordProvider,
	catalog::catalog::Catalog,
	core::interface::auth::AuthenticationProvider,
	engine::engine::StandardEngine,
	runtime::context::rng::Rng,
	transaction::transaction::Transaction,
	value::value::{Value, duration::Duration, identity::IdentityKind, uuid::Uuid7},
};
use tracing::instrument;

pub const GUEST_NAME_PREFIX: &str = "guest:";

pub const EMAIL_ATTRIBUTE: &str = "email";

pub fn guest_session_ttl() -> Duration {
	Duration::from_days(30).expect("30 days is a valid duration")
}

#[instrument(name = "uptime::guest::create", level = "info", skip(catalog, engine, clock, rng))]
pub fn create_guest(catalog: &Catalog, engine: &StandardEngine, clock: &Clock, rng: &Rng) -> Result<IdentityId, Error> {
	let name = format!("{GUEST_NAME_PREFIX}{}", Uuid7::generate(clock, rng));
	let mut txn = engine.begin_admin(IdentityId::root())?;
	let identity = catalog.create_identity(&mut txn, &name, IdentityKind::Guest, clock, rng)?;
	txn.commit()?;
	Ok(identity.id)
}

#[derive(Debug)]
pub enum PromotionError {
	NotAGuest,
	Database(Error),
}

impl From<Error> for PromotionError {
	fn from(err: Error) -> Self {
		PromotionError::Database(err)
	}
}

#[instrument(name = "uptime::guest::promote", level = "info", skip(catalog, engine, rng, password))]
pub fn promote_guest(
	catalog: &Catalog,
	engine: &StandardEngine,
	rng: &Rng,
	identity: IdentityId,
	email: &str,
	password: String,
) -> Result<(), PromotionError> {
	let properties = PasswordProvider.create(rng, &HashMap::from([("password".to_string(), password)]))?;

	let mut txn = engine.begin_admin(IdentityId::root())?;

	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn), identity)?;
	if found.map(|found| found.resolved_kind()) != Some(IdentityKind::Guest) {
		return Err(PromotionError::NotAGuest);
	}

	catalog.rename_identity(&mut txn, identity, email)?;
	catalog.promote_guest_to_user(&mut txn, identity)?;

	let attribute = catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), EMAIL_ATTRIBUTE)?
		.expect("the `email` user attribute is declared by migration 0001");
	catalog.set_identity_attribute_value(&mut txn, identity, &attribute, Value::Utf8(email.to_string()))?;

	catalog.create_authentication(&mut txn, identity, "password", properties)?;
	txn.commit()?;
	Ok(())
}

pub fn is_guest_kind(kind: &str) -> bool {
	kind == IdentityKind::Guest.as_str()
}
