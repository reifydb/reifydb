// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod login;

use reifydb::{
	auth::{
		error::GithubError,
		github::{GithubApi, GithubConfig, GithubUser},
	},
	value::Result,
};

pub struct StubGithubApi {
	pub user: GithubUser,
	pub fail_exchange: bool,
}

impl GithubApi for StubGithubApi {
	fn exchange_code(&self, _config: &GithubConfig, code: &str) -> Result<String> {
		if self.fail_exchange {
			return Err(GithubError::ExchangeFailed {
				reason: "stub exchange failure".to_string(),
			}
			.into());
		}
		assert_eq!(code, "good-code", "the oauth code from the client must reach the code exchange verbatim");
		Ok("stub-access-token".to_string())
	}

	fn fetch_user(&self, access_token: &str) -> Result<GithubUser> {
		assert_eq!(
			access_token, "stub-access-token",
			"the user fetch must use the token minted by the code exchange"
		);
		Ok(self.user.clone())
	}
}

pub fn octocat() -> GithubUser {
	GithubUser {
		id: 583231,
		login: "octocat".to_string(),
	}
}
