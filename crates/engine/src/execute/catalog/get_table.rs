// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use reifydb_storage::{UnversionedStorage, VersionedStorage};

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {}
