// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{CowVec, Version, delta::Delta, impl_event};

#[derive(Debug)]
pub struct PostCommitEvent {
	pub deltas: CowVec<Delta>,
	pub version: Version,
}

impl_event!(PostCommitEvent);
