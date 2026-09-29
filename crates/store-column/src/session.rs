// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use vortex_array::array_session;
use vortex_session::VortexSession;

pub fn new_session() -> VortexSession {
	let session = array_session();
	vortex_arrow::initialize(&session);
	vortex_alp::initialize(&session);
	vortex_fastlanes::initialize(&session);
	vortex_fsst::initialize(&session);
	vortex_onpair::initialize(&session);
	vortex_zigzag::initialize(&session);
	vortex_sequence::initialize(&session);
	vortex_runend::initialize(&session);
	vortex_sparse::initialize(&session);
	vortex_datetime_parts::initialize(&session);
	vortex_decimal_byte_parts::initialize(&session);
	session
}
