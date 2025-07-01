// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Error;

pub trait Explain {
    fn explain(query: &str) -> Result<String, Error>;
}
