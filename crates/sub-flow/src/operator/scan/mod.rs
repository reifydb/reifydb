// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod flow;
pub mod ringbuffer;
pub mod series;
pub mod table;
pub mod view;

// All types are accessed directly from their submodules:
// - crate::operator::scan::flow::PrimitiveFlowOperator
// - crate::operator::scan::ringbuffer::PrimitiveRingBufferOperator
// - crate::operator::scan::table::PrimitiveTableOperator
// - crate::operator::scan::view::PrimitiveViewOperator
