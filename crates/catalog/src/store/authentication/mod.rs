// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::interface::{catalog::authentication::Authentication, store::MultiVersionRow};
use reifydb_value::Result;
use serde_json::from_str;

use crate::{error::CatalogError, store::authentication::shape::authentication};

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_authentication<K>(multi: MultiVersionRow<K>) -> Result<Authentication> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = authentication::get_id(&bytes);
	let identity = authentication::get_identity(&bytes);
	let method = authentication::get_method(&bytes).to_string();
	let properties_json = authentication::get_properties(&bytes).to_string();

	let properties: BTreeMap<String, String> =
		from_str(&properties_json).map_err(|e| CatalogError::AuthenticationProperties {
			id,
			reason: e.to_string(),
		})?;

	Ok(Authentication {
		id,
		identity,
		method,
		properties,
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, interface::store::MultiVersionRow};
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	#[test]
	fn test_convert_rejects_corrupt_properties_json() {
		// Without this, corrupt credentials read back as an empty map and surface as a misleading missing-hash
		// error.
		let mut row = authentication::allocate();
		authentication::set_id(&mut row, 7);
		authentication::set_identity(&mut row, IdentityId::system());
		authentication::set_method(&mut row, "password");
		authentication::set_properties(&mut row, "not json");

		let multi = MultiVersionRow {
			key: (),
			bytes: row.freeze().into(),
			version: CommitVersion(1),
		};

		let err = convert_authentication(multi).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_127");
	}
}
