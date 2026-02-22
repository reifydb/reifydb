// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::common::{WindowSize, WindowType};
use reifydb_type::fragment::Fragment;

use super::{WindowConfig, WindowNode};
use crate::{Result, error::RqlError, expression::Expression};

pub fn create_tumbling_window(
	config: WindowConfig,
	group_by: Vec<Expression>,
	aggregations: Vec<Expression>,
) -> Result<WindowNode> {
	validate_tumbling_config(&config)?;

	let window_type = config.window_type.ok_or_else(|| -> reifydb_type::error::Error {
		RqlError::WindowMissingTypeOrSize {
			fragment: Fragment::None,
		}
		.into()
	})?;

	let size = config.size.ok_or_else(|| -> reifydb_type::error::Error {
		RqlError::WindowMissingTypeOrSize {
			fragment: Fragment::None,
		}
		.into()
	})?;

	let slide = None;

	Ok(WindowNode {
		window_type,
		size,
		slide,
		group_by,
		aggregations,
		min_events: config.min_events.unwrap_or(1),
		max_window_count: config.max_window_count,
		max_window_age: config.max_window_age,
	})
}

fn validate_tumbling_config(config: &WindowConfig) -> Result<()> {
	if config.slide.is_some() {
		return Err(RqlError::WindowTumblingWithSlide {
			fragment: Fragment::None,
		}
		.into());
	}

	match (&config.window_type, &config.size) {
		(Some(WindowType::Time(_)), Some(WindowSize::Duration(_))) => {}
		(Some(WindowType::Count), Some(WindowSize::Count(_))) => {}
		(Some(window_type), Some(size)) => {
			return Err(RqlError::WindowIncompatibleTypeSize {
				fragment: Fragment::None,
				window_type: format!("{:?}", window_type),
				size_type: format!("{:?}", size),
			}
			.into());
		}
		_ => {
			return Err(RqlError::WindowMissingTypeOrSize {
				fragment: Fragment::None,
			}
			.into());
		}
	}

	Ok(())
}

#[cfg(test)]
pub mod tests {
	use std::time::Duration;

	use reifydb_core::common::{WindowSize, WindowTimeMode, WindowType};

	use super::*;

	#[test]
	fn test_create_time_based_tumbling_window() {
		let config = WindowConfig {
			window_type: Some(WindowType::Time(WindowTimeMode::Processing)),
			size: Some(WindowSize::Duration(Duration::from_secs(60))),
			slide: None,
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_tumbling_window(config, vec![], vec![]);
		assert!(result.is_ok());

		let window = result.unwrap();
		assert!(matches!(window.window_type, WindowType::Time(WindowTimeMode::Processing)));
		assert!(matches!(window.size, WindowSize::Duration(_)));
		assert!(window.slide.is_none());
	}

	#[test]
	fn test_create_count_based_tumbling_window() {
		let config = WindowConfig {
			window_type: Some(WindowType::Count),
			size: Some(WindowSize::Count(100)),
			slide: None,
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_tumbling_window(config, vec![], vec![]);
		assert!(result.is_ok());

		let window = result.unwrap();
		assert!(matches!(window.window_type, WindowType::Count));
		assert!(matches!(window.size, WindowSize::Count(100)));
		assert!(window.slide.is_none());
	}

	#[test]
	fn test_tumbling_window_with_slide_fails() {
		let config = WindowConfig {
			window_type: Some(WindowType::Time(WindowTimeMode::Processing)),
			size: Some(WindowSize::Duration(Duration::from_secs(60))),
			slide: Some(reifydb_core::common::WindowSlide::Duration(Duration::from_secs(30))),
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_tumbling_window(config, vec![], vec![]);
		assert!(result.is_err());
	}

	#[test]
	fn test_tumbling_window_incompatible_type_size() {
		let config = WindowConfig {
			window_type: Some(WindowType::Time(WindowTimeMode::Processing)),
			size: Some(WindowSize::Count(100)),
			slide: None,
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_tumbling_window(config, vec![], vec![]);
		assert!(result.is_err());
	}
}
