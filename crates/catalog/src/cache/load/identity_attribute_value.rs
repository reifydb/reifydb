// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{key::identity_attribute_value::IdentityAttributeValueKey, return_internal_error};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{Result, store::identity_attribute_value::convert_identity_attribute_value};

pub(crate) fn load_identity_attribute_values(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = IdentityAttributeValueKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let value = convert_identity_attribute_value(multi)?;
		let Some(attribute) = catalog.find_identity_attribute_at(value.attribute, version) else {
			return_internal_error!(
				"identity attribute value row references unknown attribute {}",
				value.attribute
			);
		};
		if value.value.get_type() != attribute.value_type {
			return_internal_error!(
				"identity attribute value for `{}` has type {}, catalog declares {}",
				attribute.name,
				value.value.get_type(),
				attribute.value_type
			);
		}
		catalog.set_identity_attribute_value(value.identity, value.attribute, version, Some(value));
	}

	Ok(())
}
