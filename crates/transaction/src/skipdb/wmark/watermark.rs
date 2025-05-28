// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

mod std_;
pub use std_::*;

/// Error type for watermark.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WaterMarkError {
    /// The watermark is uninitialized, please call init first before using any other functions
    Uninitialized,
    /// The watermark is canceled.
    Canceled,
    /// The channel is closed.
    ChannelClosed,
}

impl core::fmt::Display for WaterMarkError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Uninitialized => write!(
                f,
                "watermark: uninitialized, please call init first before using any other functions"
            ),
            Self::Canceled => write!(f, "watermark: canceled"),
            Self::ChannelClosed => write!(f, "watermark: channel closed"),
        }
    }
}

impl std::error::Error for WaterMarkError {}

#[test]
fn test_error() {
    assert_eq!(
        format!("{}", WaterMarkError::Uninitialized),
        "watermark: uninitialized, please call init first before using any other functions"
    );

    assert_eq!(format!("{}", WaterMarkError::Canceled), "watermark: canceled");

    assert_eq!(format!("{}", WaterMarkError::ChannelClosed), "watermark: channel closed");
}
