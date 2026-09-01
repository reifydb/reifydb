// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{interface::catalog::authentication::Authentication, key::identity::AuthenticationKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::{
	error::{Diagnostic, Error},
	fragment::Fragment,
	value::identity::IdentityId,
};
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	store::{authentication::shape::authentication, sequence::system::SystemSequence},
};

impl CatalogStore {
	pub(crate) fn create_authentication(
		txn: &mut AdminTransaction,
		identity: IdentityId,
		method: &str,
		properties: HashMap<String, String>,
	) -> Result<Authentication> {
		let id = SystemSequence::next_authentication_id(txn)?;

		let properties_json = to_string(&properties).map_err(|e| {
			Error(Box::new(Diagnostic {
				code: "CT_020".to_string(),
				rql: None,
				message: format!("failed to serialize authentication properties: {}", e),
				fragment: Fragment::None,
				label: Some("serialization failed".to_string()),
				help: Some("ensure authentication properties are valid".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			}))
		})?;

		let mut row = authentication::allocate();
		authentication::set_id(&mut row, id);
		authentication::set_identity(&mut row, identity);
		authentication::set_method(&mut row, method);
		authentication::set_properties(&mut row, &properties_json);

		txn.set(&AuthenticationKey::encoded(id), row.freeze())?;

		Ok(Authentication {
			id,
			identity,
			method: method.to_string(),
			properties,
		})
	}
}
