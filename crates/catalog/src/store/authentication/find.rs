// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::authentication::{Authentication, AuthenticationId},
	key::authentication::AuthenticationKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::identity::IdentityId;

use crate::{
	CatalogStore, Result,
	store::authentication::{convert_authentication, schema::authentication},
};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_authentication(
		rx: &mut Transaction<'_>,
		id: AuthenticationId,
	) -> Result<Option<Authentication>> {
		Ok(rx.get(&AuthenticationKey::encoded(id))?.map(convert_authentication))
	}

	pub(crate) fn find_authentication_by_identity_and_method(
		rx: &mut Transaction<'_>,
		identity: IdentityId,
		method: &str,
	) -> Result<Option<Authentication>> {
		let mut stream = rx.range(AuthenticationKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let auth_identity =
				authentication::SCHEMA.get_identity_id(&multi.row, authentication::IDENTITY);
			let auth_method = authentication::SCHEMA.get_utf8(&multi.row, authentication::METHOD);
			if auth_identity == identity && auth_method == method {
				return Ok(Some(convert_authentication(multi)));
			}
		}

		Ok(None)
	}

	/// List all authentications for a given method (e.g., "token").
	pub(crate) fn list_authentications_by_method(
		rx: &mut Transaction<'_>,
		method: &str,
	) -> Result<Vec<Authentication>> {
		let mut stream = rx.range(AuthenticationKey::full_scan(), 1024)?;
		let mut results = Vec::new();

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let auth_method = authentication::SCHEMA.get_utf8(&multi.row, authentication::METHOD);
			if auth_method == method {
				results.push(convert_authentication(multi));
			}
		}

		Ok(results)
	}
}
