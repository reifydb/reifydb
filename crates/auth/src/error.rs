// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq)]
pub enum Error {}

impl Display for Error {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {}
