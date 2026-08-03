// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

pub const SCALES: [u64; 3] = [10_000, 100_000, 1_000_000];
pub const THREADS: [usize; 5] = [1, 2, 4, 8, 16];

pub struct Profile {
	pub name: String,
	pub threads: usize,
	pub stop: StopCondition,
	pub scale: Scale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopCondition {
	Iterations(u64),
	Duration(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scale {
	Rows(u64),
	Fixed,
}

impl Scale {
	pub fn rows(&self) -> u64 {
		match self {
			Scale::Rows(rows) => *rows,
			Scale::Fixed => 0,
		}
	}

	pub fn label(&self) -> String {
		match self {
			Scale::Fixed => "fixed".to_string(),
			Scale::Rows(rows) => scale_label(*rows),
		}
	}
}

pub fn scale_label(rows: u64) -> String {
	if rows >= 1_000_000 && rows.is_multiple_of(1_000_000) {
		format!("{}m", rows / 1_000_000)
	} else if rows >= 1_000 && rows.is_multiple_of(1_000) {
		format!("{}k", rows / 1_000)
	} else {
		rows.to_string()
	}
}

impl Profile {
	pub fn scaled(threads: usize, rows: u64, stop: StopCondition) -> Self {
		Self {
			name: format!("t{}_{}", threads, scale_label(rows)),
			threads,
			stop,
			scale: Scale::Rows(rows),
		}
	}

	pub fn fixed(threads: usize, stop: StopCondition) -> Self {
		Self {
			name: format!("t{}", threads),
			threads,
			stop,
			scale: Scale::Fixed,
		}
	}
}

pub fn scaled_matrix(threads: &[usize], scales: &[u64], stop: StopCondition) -> Vec<Profile> {
	let mut profiles = Vec::with_capacity(threads.len() * scales.len());
	for rows in scales {
		for count in threads {
			profiles.push(Profile::scaled(*count, *rows, stop));
		}
	}
	profiles
}

pub fn fixed_matrix(threads: &[usize], stop: StopCondition) -> Vec<Profile> {
	threads.iter().map(|count| Profile::fixed(*count, stop)).collect()
}

#[cfg(test)]
mod tests {
	use crate::profile::{Profile, Scale, StopCondition, fixed_matrix, scale_label, scaled_matrix};

	#[test]
	fn scale_labels_stay_readable_at_each_benchmark_size() {
		assert_eq!(scale_label(10_000), "10k");
		assert_eq!(scale_label(100_000), "100k");
		assert_eq!(scale_label(1_000_000), "1m");
	}

	#[test]
	fn scale_label_falls_back_to_the_raw_count_when_not_a_round_number() {
		// A custom scale must still produce a distinct profile name, otherwise two profiles
		// would collide and validation would reject an otherwise valid scenario.
		assert_eq!(scale_label(12_345), "12345");
		assert_eq!(scale_label(1_500_000), "1500k");
	}

	#[test]
	fn scaled_profile_names_encode_both_axes() {
		let profile = Profile::scaled(4, 100_000, StopCondition::Iterations(1));
		assert_eq!(profile.name, "t4_100k");
		assert_eq!(profile.scale.rows(), 100_000);
	}

	#[test]
	fn fixed_profile_reports_zero_rows() {
		// Manual datasets have the size their literal rows give them; a fixed profile must not
		// imply a generated row count to the seeder.
		let profile = Profile::fixed(8, StopCondition::Iterations(1));
		assert_eq!(profile.name, "t8");
		assert_eq!(profile.scale, Scale::Fixed);
		assert_eq!(profile.scale.rows(), 0);
	}

	#[test]
	fn scaled_matrix_covers_every_thread_and_scale_pair_uniquely() {
		let profiles = scaled_matrix(&[1, 4], &[10_000, 1_000_000], StopCondition::Iterations(1));

		assert_eq!(profiles.len(), 4);
		let mut names: Vec<&str> = profiles.iter().map(|profile| profile.name.as_str()).collect();
		names.sort();
		assert_eq!(names, vec!["t1_10k", "t1_1m", "t4_10k", "t4_1m"]);
	}

	#[test]
	fn fixed_matrix_produces_one_profile_per_thread_count() {
		let profiles = fixed_matrix(&[1, 2], StopCondition::Iterations(1));
		let names: Vec<&str> = profiles.iter().map(|profile| profile.name.as_str()).collect();
		assert_eq!(names, vec!["t1", "t2"]);
	}
}
