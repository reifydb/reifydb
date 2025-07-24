// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::delta::Delta;
use crate::{CowVec, Version, impl_hook};

pub struct PreCommitHook {
    pub deltas: CowVec<Delta>,
    pub version: Version,
}

impl_hook!(PreCommitHook);

pub struct PostCommitHook {
    pub deltas: CowVec<Delta>,
    pub version: Version,
}

impl_hook!(PostCommitHook);
