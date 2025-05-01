// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Represents errors that may occur during binary encoding or decoding operations
/// within key-value storage or network protocol contexts.
///
/// This enum captures encoding-related failures such as malformed binary input,
/// unsupported data formats, or internal encoding bugs. It is designed to provide
/// precise error feedback for low-level serialization and deserialization logic.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {}

impl Display for Error {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}
