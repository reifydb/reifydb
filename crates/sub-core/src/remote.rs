// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(feature = "remote"))]
pub(crate) use disabled::RemoteSubscription;
#[cfg(all(not(feature = "remote"), not(reifydb_single_threaded)))]
pub(crate) use disabled::{connect_remote, proxy_remote_to_sink};
#[cfg(feature = "remote")]
pub(crate) use reifydb_remote_proxy::RemoteSubscription;
#[cfg(all(feature = "remote", not(reifydb_single_threaded)))]
pub(crate) use reifydb_remote_proxy::{connect_remote, proxy_remote_to_sink};

#[cfg(not(feature = "remote"))]
mod disabled {
	use std::fmt;

	use reifydb_client::{RawChangePayload, SubscriptionConfig, WireFormat};
	use reifydb_value::error::Error;
	use tokio::sync::watch;

	#[derive(Debug)]
	pub struct RemoteSubscriptionError;

	impl fmt::Display for RemoteSubscriptionError {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			write!(f, "Remote subscriptions require the `remote` feature of reifydb-sub-core")
		}
	}

	pub enum RemoteSubscription {}

	impl RemoteSubscription {
		pub fn subscription_id(&self) -> &str {
			match *self {}
		}
	}

	pub async fn connect_remote(
		_address: &str,
		_body: &str,
		_config: SubscriptionConfig,
		_token: Option<&str>,
		_wire_format: WireFormat,
	) -> Result<RemoteSubscription, RemoteSubscriptionError> {
		Err(RemoteSubscriptionError)
	}

	pub async fn proxy_remote_to_sink<F>(
		remote_sub: RemoteSubscription,
		_shutdown: watch::Receiver<bool>,
		_sink: F,
	) -> Result<(), Error>
	where
		F: FnMut(RawChangePayload) -> bool + Send + 'static,
	{
		match remote_sub {}
	}
}
