// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use vortex_alp::initialize as alp_initialize;
use vortex_array::array_session;
use vortex_arrow::initialize as arrow_initialize;
use vortex_datetime_parts::initialize as datetime_parts_initialize;
use vortex_decimal_byte_parts::initialize as decimal_byte_parts_initialize;
use vortex_fastlanes::initialize as fastlanes_initialize;
use vortex_fsst::initialize as fsst_initialize;
use vortex_onpair::initialize as onpair_initialize;
use vortex_runend::initialize as runend_initialize;
use vortex_sequence::initialize as sequence_initialize;
use vortex_session::VortexSession;
use vortex_sparse::initialize as sparse_initialize;
use vortex_zigzag::initialize as zigzag_initialize;

pub fn new_session() -> VortexSession {
	let session = array_session();
	arrow_initialize(&session);
	alp_initialize(&session);
	fastlanes_initialize(&session);
	fsst_initialize(&session);
	onpair_initialize(&session);
	zigzag_initialize(&session);
	sequence_initialize(&session);
	runend_initialize(&session);
	sparse_initialize(&session);
	datetime_parts_initialize(&session);
	decimal_byte_parts_initialize(&session);
	session
}
