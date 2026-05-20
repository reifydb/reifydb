// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

mod client;
pub mod generated;

pub use client::{
	BatchFramesEnvelope, BatchGrpcSubscription, BatchMemberHandle, BatchStreamEvent, GrpcChange, GrpcClient,
	GrpcSubscription, RawChangePayload,
};
