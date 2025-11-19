// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{CommitVersion, impl_event, interface::CdcConsumerId};

/// Emitted when a CDC consumer's checkpoint advances to a new version
#[derive(Debug, Clone)]
pub struct CdcCheckpointAdvancedEvent {
	/// The consumer whose checkpoint advanced
	pub consumer_id: CdcConsumerId,
	/// The new checkpoint version
	pub version: CommitVersion,
}

impl_event!(CdcCheckpointAdvancedEvent);
