// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
mod layout;

pub(crate) use reifydb_core::{delta::Delta, interface::CdcChange, value::encoded::EncodedValues};

/// Generate a CDC change from a Delta
pub(crate) fn generate_cdc_change(delta: Delta, pre: Option<EncodedValues>) -> CdcChange {
	match delta {
		Delta::Set {
			key,
			values,
		} => {
			if let Some(pre) = pre {
				CdcChange::Update {
					key,
					pre,
					post: values,
				}
			} else {
				CdcChange::Insert {
					key,
					post: values,
				}
			}
		}

		Delta::Remove {
			key,
		} => CdcChange::Delete {
			key,
			pre,
		},
	}
}
