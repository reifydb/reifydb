// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

const LANES: usize = 8;

pub(crate) fn dot(left: &[f32], right: &[f32]) -> f32 {
	let mut lanes = [0.0f32; LANES];
	let mut left_chunks = left.chunks_exact(LANES);
	let mut right_chunks = right.chunks_exact(LANES);

	for (l, r) in left_chunks.by_ref().zip(right_chunks.by_ref()) {
		for i in 0..LANES {
			lanes[i] += l[i] * r[i];
		}
	}

	let mut total: f32 = lanes.iter().sum();
	for (l, r) in left_chunks.remainder().iter().zip(right_chunks.remainder().iter()) {
		total += l * r;
	}
	total
}

pub(crate) fn squared_norm(value: &[f32]) -> f32 {
	dot(value, value)
}

pub(crate) fn norm(value: &[f32]) -> f32 {
	squared_norm(value).sqrt()
}

pub(crate) fn l2_distance(left: &[f32], right: &[f32]) -> f32 {
	let mut lanes = [0.0f32; LANES];
	let mut left_chunks = left.chunks_exact(LANES);
	let mut right_chunks = right.chunks_exact(LANES);

	for (l, r) in left_chunks.by_ref().zip(right_chunks.by_ref()) {
		for i in 0..LANES {
			let delta = l[i] - r[i];
			lanes[i] += delta * delta;
		}
	}

	let mut total: f32 = lanes.iter().sum();
	for (l, r) in left_chunks.remainder().iter().zip(right_chunks.remainder().iter()) {
		let delta = l - r;
		total += delta * delta;
	}
	total.sqrt()
}

pub(crate) fn cosine_distance(left: &[f32], right: &[f32]) -> Option<f32> {
	let denominator = squared_norm(left).sqrt() * squared_norm(right).sqrt();
	if denominator == 0.0 {
		return None;
	}
	Some(1.0 - (dot(left, right) / denominator))
}

#[cfg(test)]
mod tests {
	use super::{cosine_distance, dot, l2_distance, norm};

	// Every kernel folds 8 lanes at a time and then sweeps a scalar remainder. A dimension below 8
	// produces ZERO full chunks, so a kernel that forgets the remainder silently reads nothing:
	// dot/l2/norm collapse to 0.0 and cosine computes 1.0 - 0/0 = NaN. vector(4) is the dimension
	// the smoke table uses, so these cases are the difference between working and NaN in production.
	const SHORT: usize = 4;

	#[test]
	fn dot_sums_the_remainder_when_dims_are_below_one_lane() {
		let left = [1.0, 2.0, 3.0, 4.0];
		let right = [5.0, 6.0, 7.0, 8.0];
		assert_eq!(left.len(), SHORT);
		assert_eq!(dot(&left, &right), 70.0);
	}

	#[test]
	fn dot_sums_chunks_and_remainder_together() {
		let left: Vec<f32> = (1..=12).map(|v| v as f32).collect();
		let right = vec![1.0f32; 12];
		assert_eq!(dot(&left, &right), 78.0);
	}

	#[test]
	fn dot_of_orthogonal_vectors_is_zero() {
		assert_eq!(dot(&[1.0, 0.0, 0.0, 0.0], &[0.0, 1.0, 0.0, 0.0]), 0.0);
	}

	#[test]
	fn norm_covers_the_remainder() {
		assert_eq!(norm(&[3.0, 4.0]), 5.0);
		assert_eq!(norm(&[0.0, 0.0, 0.0, 0.0]), 0.0);
	}

	#[test]
	fn norm_covers_chunks_and_remainder() {
		let value = vec![1.0f32; 12];
		assert_eq!(norm(&value), 12.0f32.sqrt());
	}

	#[test]
	fn l2_distance_covers_the_remainder() {
		assert_eq!(l2_distance(&[0.0, 0.0], &[3.0, 4.0]), 5.0);
		assert_eq!(l2_distance(&[1.0, 2.0, 3.0, 4.0], &[1.0, 2.0, 3.0, 4.0]), 0.0);
	}

	#[test]
	fn l2_distance_covers_chunks_and_remainder() {
		let left = vec![0.0f32; 12];
		let right = vec![1.0f32; 12];
		assert_eq!(l2_distance(&left, &right), 12.0f32.sqrt());
	}

	// The regression that plan-4's kernel sketch shipped: chunks_exact(8) with no remainder pass
	// returns NaN for every vector(N < 8). A NaN distance poisons the SORT that KNN depends on.
	#[test]
	fn cosine_distance_of_a_short_vector_is_not_nan() {
		let value = [0.1, 0.2, 0.3, 0.4];
		let distance = cosine_distance(&value, &value).expect("a non-zero vector has a defined distance");
		assert!(!distance.is_nan(), "vector({SHORT}) must not compute 0/0");
		assert!(distance.abs() <= 1e-6, "self-distance must be ~0, got {distance}");
	}

	// Self-distance is exact only up to one ulp of 1.0, and the error can land on EITHER side of
	// zero. Asserting dist >= 0.0 would be wrong.
	#[test]
	fn cosine_self_distance_may_be_negative_but_is_within_one_ulp() {
		for value in [
			vec![0.1f32, 0.2, 0.3, 0.4],
			vec![0.1f32, 0.1, 0.1, 0.3],
			vec![1.0f32; 12],
			(1..=768).map(|v| v as f32 / 768.0).collect(),
		] {
			let distance = cosine_distance(&value, &value).expect("non-zero vector");
			assert!(distance.abs() <= f32::EPSILON, "self-distance {distance} exceeds one ulp");
		}
	}

	#[test]
	fn cosine_distance_of_orthogonal_vectors_is_one() {
		let distance = cosine_distance(&[1.0, 0.0, 0.0, 0.0], &[0.0, 1.0, 0.0, 0.0]).unwrap();
		assert!((distance - 1.0).abs() <= 1e-6, "orthogonal vectors are distance 1, got {distance}");
	}

	#[test]
	fn cosine_distance_of_opposite_vectors_is_two() {
		let distance = cosine_distance(&[1.0, 2.0, 3.0, 4.0], &[-1.0, -2.0, -3.0, -4.0]).unwrap();
		assert!((distance - 2.0).abs() <= 1e-6, "opposed vectors are distance 2, got {distance}");
	}

	// A zero vector has no direction, so cosine distance is undefined rather than 0, 1, or NaN.
	// The function layer turns this None into a NONE cell.
	#[test]
	fn cosine_distance_of_a_zero_vector_is_undefined() {
		assert_eq!(cosine_distance(&[0.0, 0.0, 0.0, 0.0], &[1.0, 2.0, 3.0, 4.0]), None);
		assert_eq!(cosine_distance(&[1.0, 2.0, 3.0, 4.0], &[0.0, 0.0, 0.0, 0.0]), None);
		assert_eq!(cosine_distance(&[0.0, 0.0], &[0.0, 0.0]), None);
	}
}
