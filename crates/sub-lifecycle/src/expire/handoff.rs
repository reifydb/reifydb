// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{actors::flow::FlowActorMessage, delta::RemoveVisibility, interface::catalog::id::ViewId};
use reifydb_engine::engine::StandardEngine;

pub(crate) enum HandOff {
	Sent,
	Skipped,
}

pub(crate) fn hand_off(
	engine: &StandardEngine,
	view: ViewId,
	keys: Vec<EncodedKey>,
	visibility: RemoveVisibility,
) -> HandOff {
	let Some(actor) = engine.flow_writers().find(view) else {
		return HandOff::Skipped;
	};
	match actor.send(FlowActorMessage::RemoveExpired {
		keys,
		visibility,
	}) {
		Ok(()) => HandOff::Sent,
		Err(_) => HandOff::Skipped,
	}
}
