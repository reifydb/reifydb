// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::identity_attribute::IdentityAttributeKey;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{Result, store::identity_attribute::convert_identity_attribute};

pub(crate) fn load_identity_attributes(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = IdentityAttributeKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let attribute = convert_identity_attribute(multi);
		catalog.set_identity_attribute(attribute.id, version, Some(attribute));
	}

	Ok(())
}
