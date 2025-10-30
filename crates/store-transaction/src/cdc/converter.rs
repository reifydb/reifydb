// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, Result,
	interface::{Cdc, CdcChange, CdcSequencedChange},
	value::encoded::EncodedValues,
};

use crate::{
	backend::{multi::BackendMultiVersionGet, result::MultiVersionGetResult},
	cdc::{InternalCdc, InternalCdcChange},
};

/// Trait for converting internal CDC representation to public CDC by resolving values
pub trait CdcConverter {
	/// Convert an internal CDC (with version references) to a public CDC (with resolved values)
	fn convert(&self, internal: InternalCdc) -> Result<Cdc>;
}

/// Implementation for any type that supports multi-version get operations
impl<T> CdcConverter for T
where
	T: BackendMultiVersionGet,
{
	fn convert(&self, internal: InternalCdc) -> Result<Cdc> {
		let mut changes = Vec::with_capacity(internal.changes.len());

		for internal_change in internal.changes {
			let resolved_change = match &internal_change.change {
				InternalCdcChange::Insert {
					key,
					post_version,
				} => {
					// Fetch the post value at the given version
					let post_result = self.get(key, *post_version)?;
					let post = match post_result {
						MultiVersionGetResult::Value(mv) => mv.values,
						_ => EncodedValues(CowVec::new(vec![])), // Handle tombstone/not found
					};
					CdcChange::Insert {
						key: key.clone(),
						post,
					}
				}
				InternalCdcChange::Update {
					key,
					pre_version,
					post_version,
				} => {
					// Fetch both pre and post values
					let pre_result = self.get(key, *pre_version)?;
					let pre = match pre_result {
						MultiVersionGetResult::Value(mv) => mv.values,
						_ => EncodedValues(CowVec::new(vec![])),
					};

					let post_result = self.get(key, *post_version)?;
					let post = match post_result {
						MultiVersionGetResult::Value(mv) => mv.values,
						_ => EncodedValues(CowVec::new(vec![])),
					};

					CdcChange::Update {
						key: key.clone(),
						pre,
						post,
					}
				}
				InternalCdcChange::Delete {
					key,
					pre_version,
				} => {
					// Fetch the pre value
					let pre_result = self.get(key, *pre_version)?;
					let pre = match pre_result {
						MultiVersionGetResult::Value(mv) => Some(mv.values),
						_ => None,
					};
					CdcChange::Delete {
						key: key.clone(),
						pre,
					}
				}
			};

			changes.push(CdcSequencedChange {
				sequence: internal_change.sequence,
				change: resolved_change,
			});
		}

		Ok(Cdc {
			version: internal.version,
			timestamp: internal.timestamp,
			changes,
		})
	}
}
