// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::common::{WindowSize, WindowSlide, WindowType};
use reifydb_type::{error::Error, fragment::Fragment};

use super::{WindowConfig, WindowNode};
use crate::{Result, error::RqlError, expression::Expression};

pub fn create_sliding_window(
	config: WindowConfig,
	group_by: Vec<Expression>,
	aggregations: Vec<Expression>,
) -> Result<WindowNode> {
	validate_sliding_config(&config)?;

	let window_type = config.window_type.ok_or_else(|| -> Error {
		RqlError::WindowMissingTypeOrSize {
			fragment: Fragment::None,
		}
		.into()
	})?;

	let size = config.size.ok_or_else(|| -> Error {
		RqlError::WindowMissingTypeOrSize {
			fragment: Fragment::None,
		}
		.into()
	})?;

	let slide = config.slide;

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

fn validate_sliding_config(config: &WindowConfig) -> Result<()> {
	let slide = config.slide.as_ref().ok_or_else(|| -> Error {
		RqlError::WindowMissingSlideParameter {
			fragment: Fragment::None,
		}
		.into()
	})?;

	match (&config.window_type, &config.size) {
		(Some(WindowType::Time(_)), Some(WindowSize::Duration(window_duration))) => {
			if let WindowSlide::Duration(slide_duration) = slide {
				if slide_duration >= window_duration {
					return Err(RqlError::WindowSlideTooLarge {
						fragment: Fragment::None,
						slide_value: format!("{:?}", slide_duration),
						window_value: format!("{:?}", window_duration),
					}
					.into());
				}
			} else {
				return Err(RqlError::WindowIncompatibleSlideType {
					fragment: Fragment::None,
					window_type: "time-based".to_string(),
					slide_type: "count-based".to_string(),
				}
				.into());
			}
		}
		(Some(WindowType::Count), Some(WindowSize::Count(window_count))) => {
			if let WindowSlide::Count(slide_count) = slide {
				if slide_count >= window_count {
					return Err(RqlError::WindowSlideTooLarge {
						fragment: Fragment::None,
						slide_value: slide_count.to_string(),
						window_value: window_count.to_string(),
					}
					.into());
				}
			} else {
				return Err(RqlError::WindowIncompatibleSlideType {
					fragment: Fragment::None,
					window_type: "count-based".to_string(),
					slide_type: "time-based".to_string(),
				}
				.into());
			}
		}
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

	use reifydb_core::common::{WindowSize, WindowSlide, WindowTimeMode, WindowType};

	use super::*;

	#[test]
	fn test_create_time_based_sliding_window() {
		let config = WindowConfig {
			window_type: Some(WindowType::Time(WindowTimeMode::Processing)),
			size: Some(WindowSize::Duration(Duration::from_secs(300))),
			slide: Some(WindowSlide::Duration(Duration::from_secs(60))),
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_sliding_window(config, vec![], vec![]);
		assert!(result.is_ok());

		let window = result.unwrap();
		assert!(matches!(window.window_type, WindowType::Time(WindowTimeMode::Processing)));
		assert!(matches!(window.size, WindowSize::Duration(_)));
		assert!(window.slide.is_some());
	}

	#[test]
	fn test_create_count_based_sliding_window() {
		let config = WindowConfig {
			window_type: Some(WindowType::Count),
			size: Some(WindowSize::Count(100)),
			slide: Some(WindowSlide::Count(20)),
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_sliding_window(config, vec![], vec![]);
		assert!(result.is_ok());

		let window = result.unwrap();
		assert!(matches!(window.window_type, WindowType::Count));
		assert!(matches!(window.size, WindowSize::Count(100)));
		assert!(matches!(window.slide, Some(WindowSlide::Count(20))));
	}

	#[test]
	fn test_sliding_window_without_slide_fails() {
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

		let result = create_sliding_window(config, vec![], vec![]);
		assert!(result.is_err());
	}

	#[test]
	fn test_sliding_window_slide_larger_than_window_fails() {
		let config = WindowConfig {
			window_type: Some(WindowType::Time(WindowTimeMode::Processing)),
			size: Some(WindowSize::Duration(Duration::from_secs(60))),
			slide: Some(WindowSlide::Duration(Duration::from_secs(120))),
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_sliding_window(config, vec![], vec![]);
		assert!(result.is_err());
	}

	#[test]
	fn test_sliding_window_slide_equal_to_window_fails() {
		let config = WindowConfig {
			window_type: Some(WindowType::Time(WindowTimeMode::Processing)),
			size: Some(WindowSize::Duration(Duration::from_secs(60))),
			slide: Some(WindowSlide::Duration(Duration::from_secs(60))),
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_sliding_window(config, vec![], vec![]);
		assert!(result.is_err());
	}

	#[test]
	fn test_sliding_window_incompatible_slide_type_fails() {
		let config = WindowConfig {
			window_type: Some(WindowType::Time(WindowTimeMode::Processing)),
			size: Some(WindowSize::Duration(Duration::from_secs(300))),
			slide: Some(WindowSlide::Count(20)),
			timestamp_column: None,
			min_events: None,
			max_window_count: None,
			max_window_age: None,
			is_rolling: false,
		};

		let result = create_sliding_window(config, vec![], vec![]);
		assert!(result.is_err());
	}
}
