// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod client;
pub mod generated;

pub use client::{
	BatchFramesEnvelope, BatchGrpcSubscription, BatchMemberHandle, BatchStreamEvent, GrpcChange, GrpcClient,
	GrpcSubscription, RawChangePayload,
};
