// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::authentication::Authentication, key::authentication::AuthenticationKey};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

use crate::{
	CatalogStore, Result,
	store::authentication::{convert_authentication, shape::authentication},
};

impl CatalogStore {
	pub(crate) fn list_all_authentications(rx: &mut Transaction<'_>) -> Result<Vec<Authentication>> {
		let mut result = Vec::new();
		let stream = rx.range(AuthenticationKey::full_scan(), 1024)?;

		for entry in stream {
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
		let stream = rx.range(AuthenticationKey::full_scan(), 1024)?;

		for entry in stream {
			let multi = entry?;
			let auth_identity = authentication::SHAPE.get_identity_id(&multi.row, authentication::IDENTITY);
			if auth_identity == identity {
				result.push(convert_authentication(multi));
			}
		}

		Ok(result)
	}
}
