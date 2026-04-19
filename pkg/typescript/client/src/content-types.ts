// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Canonical content-type constants for ReifyDB wire formats.
// Mirrors the Rust constants in crates/sub-server/src/response.rs.

/** Reifydb's row-oriented JSON format. */
export const CONTENT_TYPE_JSON = "application/vnd.reifydb.json";

/** Reifydb's columnar frames wire format ({frames: [...]}). */
export const CONTENT_TYPE_FRAMES = "application/vnd.reifydb.frames";

/** Reifydb's binary columnar format (RBCF). */
export const CONTENT_TYPE_RBCF = "application/vnd.reifydb.rbcf";

/** Reifydb's protobuf frame format (used on gRPC). */
export const CONTENT_TYPE_PROTO = "application/vnd.reifydb.proto";
