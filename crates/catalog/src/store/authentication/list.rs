// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::authentication::Authentication, key::authentication::AuthenticationKey};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::identity::IdentityId;

use crate::{
	CatalogStore, Result,
	store::authentication::{convert_authentication, schema::authentication},
};

impl CatalogStore {
	pub(crate) fn list_all_authentications(rx: &mut Transaction<'_>) -> Result<Vec<Authentication>> {
		let mut result = Vec::new();
		let mut stream = rx.range(AuthenticationKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_authentication(multi));
		}

		Ok(result)
	}

	#[allow(dead_code)]
	pub(crate) fn list_authentications_by_identity(
		rx: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Vec<Authentication>> {
		let mut result = Vec::new();
		let mut stream = rx.range(AuthenticationKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let auth_identity =
				authentication::SCHEMA.get_identity_id(&multi.row, authentication::IDENTITY);
			if auth_identity == identity {
				result.push(convert_authentication(multi));
			}
		}

		Ok(result)
	}
}
