// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod bitvec;
pub mod clock;
pub mod cowvec;
pub mod either;
pub mod encoding;
pub mod ioc;
pub mod retry;
mod versioned;
pub mod wait_group;

pub use bitvec::BitVec;
#[cfg(debug_assertions)]
pub use clock::{
    mock_time_advance, mock_time_advance_micros, mock_time_advance_millis,
    mock_time_advance_nanos, mock_time_clear,
    mock_time_get, mock_time_get_micros, mock_time_get_millis,
    mock_time_get_nanos, mock_time_is_active, mock_time_scoped,
    mock_time_scoped_micros, mock_time_scoped_millis, mock_time_scoped_nanos,
    mock_time_set, mock_time_set_micros, mock_time_set_millis,
    mock_time_set_nanos, mock_time_with, mock_time_with_control,
    mock_time_with_micros, mock_time_with_millis, mock_time_with_nanos,
    MockTimeControl, MockTimeGuard,
};
pub use clock::{now, now_micros, now_millis, now_nanos};
pub use cowvec::CowVec;
pub use either::Either;
pub use retry::retry;
pub use versioned::VersionedContainer;
pub use wait_group::WaitGroup;
