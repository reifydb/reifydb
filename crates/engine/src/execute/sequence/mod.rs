// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod schema;

use reifydb_core::catalog::SequenceId;

pub(crate) const SCHEMA_SEQUENCE_ID: SequenceId = SequenceId(1);
pub(crate) const TABLE_SEQUENCE_ID: SequenceId = SequenceId(2);
