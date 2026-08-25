// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Console subsystem. Dials out to Console over WSS and multiplexes lanes with yamux: lane 0 carries plaintext
//! control frames, lanes 1 and above carry byte streams sealed against Console itself.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
