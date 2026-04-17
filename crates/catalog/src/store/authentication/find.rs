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
	store::authentication::{convert_authentication, shape::authentication},
};

impl CatalogStore {
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
		let stream = rx.range(AuthenticationKey::full_scan(), 1024)?;

		for entry in stream {
			let multi = entry?;
			let auth_identity = authentication::SHAPE.get_identity_id(&multi.row, authentication::IDENTITY);
			let auth_method = authentication::SHAPE.get_utf8(&multi.row, authentication::METHOD);
			if auth_identity == identity && auth_method == method {
				return Ok(Some(convert_authentication(multi)));
			}
		}

		Ok(None)
	}
}
