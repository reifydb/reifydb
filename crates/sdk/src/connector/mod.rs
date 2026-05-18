// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Source and sink connectors that move data between flows and the outside world. A source produces rows from an
//! external system into a flow; a sink consumes rows from a flow and pushes them somewhere external. Both share the
//! same authoring shape so an extension implements either side - or both - against a single set of primitives.

pub mod sink;
pub mod source;
