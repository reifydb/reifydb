// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::common::CommitVersion;
use reifydb_value::error::ValueError;

pub trait MultiStoreHooks: Send + Sync {
	fn before_get(&self, keys: &[EncodedKey], version: CommitVersion) -> Result<(), ValueError>;
}
