// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::{interface::catalog::authentication::Authentication, key::authentication::AuthenticationKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{
	error::{Diagnostic, Error},
	fragment::Fragment,
	value::identity::IdentityId,
};
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	store::{
		authentication::shape::authentication::{ID, IDENTITY, METHOD, PROPERTIES, SHAPE},
		sequence::system::SystemSequence,
	},
};

impl CatalogStore {
	pub(crate) fn create_authentication(
		txn: &mut AdminTransaction,
		identity: IdentityId,
		method: &str,
		properties: HashMap<String, String>,
	) -> Result<Authentication> {
		let id = SystemSequence::next_authentication_id(txn)?;

		// Serialize properties as JSON
		let properties_json = to_string(&properties).map_err(|e| {
			Error(Box::new(Diagnostic {
				code: "CT_020".to_string(),
				statement: None,
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

		let mut row = SHAPE.allocate();
		SHAPE.set_u64(&mut row, ID, id);
		SHAPE.set_identity_id(&mut row, IDENTITY, identity);
		SHAPE.set_utf8(&mut row, METHOD, method);
		SHAPE.set_utf8(&mut row, PROPERTIES, &properties_json);

		txn.set(&AuthenticationKey::encoded(id), row)?;

		Ok(Authentication {
			id,
			identity,
			method: method.to_string(),
			properties,
		})
	}
}
