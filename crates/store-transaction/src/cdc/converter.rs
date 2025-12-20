// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	CowVec, Result,
	interface::{Cdc, CdcChange, CdcSequencedChange},
	value::encoded::EncodedValues,
};

use crate::{
	MultiVersionGet,
	cdc::{InternalCdc, InternalCdcChange},
};

/// Trait for converting internal CDC representation to public CDC by resolving values
#[async_trait]
pub trait CdcConverter {
	/// Convert an internal CDC (with version references) to a public CDC (with resolved values)
	async fn convert(&self, internal: InternalCdc) -> Result<Cdc>;
}

/// Implementation for StandardTransactionStore which uses MultiVersionGet
#[async_trait]
impl CdcConverter for crate::store::StandardTransactionStore {
	async fn convert(&self, internal: InternalCdc) -> Result<Cdc> {
		let mut changes = Vec::with_capacity(internal.changes.len());

		for internal_change in internal.changes {
			let resolved_change = match &internal_change.change {
				InternalCdcChange::Insert {
					key,
					post_version,
				} => {
					// Fetch the post value at the given version
					let post = match MultiVersionGet::get(self, key, *post_version).await? {
						Some(mv) => mv.values,
						None => EncodedValues(CowVec::new(vec![])),
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
					let pre = match MultiVersionGet::get(self, key, *pre_version).await? {
						Some(mv) => mv.values,
						None => EncodedValues(CowVec::new(vec![])),
					};

					let post = match MultiVersionGet::get(self, key, *post_version).await? {
						Some(mv) => mv.values,
						None => EncodedValues(CowVec::new(vec![])),
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
					let pre = MultiVersionGet::get(self, key, *pre_version)
						.await?
						.map(|mv| mv.values);
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
