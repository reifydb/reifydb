// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::time::Duration;

use reifydb::{Params, SqliteConfig, WithSubsystem, embedded};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
	let mut db = embedded::sqlite(SqliteConfig::in_memory()).await.unwrap().with_flow(|f| f).build().await.unwrap();

	db.start().await.unwrap();

	db.command_as_root("create namespace test;", Params::None).await.unwrap();

	db.command_as_root("create table test.source { id: int4, value: int4 }", Params::None).await.unwrap();

	db.command_as_root(
		r#"create deferred view test.even_numbers { id: int4, value: int4 } as {
			from test.source
			filter { (value / 2) * 2 == value }
		}"#,
		Params::None,
	)
	.await
	.unwrap();

	println!("start inserts");

	db.command_as_root(
		r#"from [
			{id: 1, value: 1}, {id: 2, value: 2}, {id: 3, value: 3}, {id: 4, value: 4}, {id: 5, value: 5},
			{id: 6, value: 6}, {id: 7, value: 7}, {id: 8, value: 8}, {id: 9, value: 9}, {id: 10, value: 10},
			{id: 11, value: 11}, {id: 12, value: 12}, {id: 13, value: 13}, {id: 14, value: 14}, {id: 15, value: 15},
			{id: 16, value: 16}, {id: 17, value: 17}, {id: 18, value: 18}, {id: 19, value: 19}, {id: 20, value: 20},
			{id: 21, value: 21}, {id: 22, value: 22}, {id: 23, value: 23}, {id: 24, value: 24}, {id: 25, value: 25},
			{id: 26, value: 26}, {id: 27, value: 27}, {id: 28, value: 28}, {id: 29, value: 29}, {id: 30, value: 30},
			{id: 31, value: 31}, {id: 32, value: 32}, {id: 33, value: 33}, {id: 34, value: 34}, {id: 35, value: 35},
			{id: 36, value: 36}, {id: 37, value: 37}, {id: 38, value: 38}, {id: 39, value: 39}, {id: 40, value: 40},
			{id: 41, value: 41}, {id: 42, value: 42}, {id: 43, value: 43}, {id: 44, value: 44}, {id: 45, value: 45},
			{id: 46, value: 46}, {id: 47, value: 47}, {id: 48, value: 48}, {id: 49, value: 49}, {id: 50, value: 50}
		] insert test.source"#,
		Params::None,
	)
	.await
	.unwrap();

	db.command_as_root(
		r#"from [
			{id: 51, value: 51}, {id: 52, value: 52}, {id: 53, value: 53}, {id: 54, value: 54}, {id: 55, value: 55},
			{id: 56, value: 56}, {id: 57, value: 57}, {id: 58, value: 58}, {id: 59, value: 59}, {id: 60, value: 60},
			{id: 61, value: 61}, {id: 62, value: 62}, {id: 63, value: 63}, {id: 64, value: 64}, {id: 65, value: 65},
			{id: 66, value: 66}, {id: 67, value: 67}, {id: 68, value: 68}, {id: 69, value: 69}, {id: 70, value: 70},
			{id: 71, value: 71}, {id: 72, value: 72}, {id: 73, value: 73}, {id: 74, value: 74}, {id: 75, value: 75},
			{id: 76, value: 76}, {id: 77, value: 77}, {id: 78, value: 78}, {id: 79, value: 79}, {id: 80, value: 80},
			{id: 81, value: 81}, {id: 82, value: 82}, {id: 83, value: 83}, {id: 84, value: 84}, {id: 85, value: 85},
			{id: 86, value: 86}, {id: 87, value: 87}, {id: 88, value: 88}, {id: 89, value: 89}, {id: 90, value: 90},
			{id: 91, value: 91}, {id: 92, value: 92}, {id: 93, value: 93}, {id: 94, value: 94}, {id: 95, value: 95},
			{id: 96, value: 96}, {id: 97, value: 97}, {id: 98, value: 98}, {id: 99, value: 99}, {id: 100, value: 100}
		] insert test.source"#,
		Params::None,
	)
	.await
	.unwrap();

	db.command_as_root(
		r#"from [
			{id: 101, value: 101}, {id: 102, value: 102}, {id: 103, value: 103}, {id: 104, value: 104}, {id: 105, value: 105},
			{id: 106, value: 106}, {id: 107, value: 107}, {id: 108, value: 108}, {id: 109, value: 109}, {id: 110, value: 110},
			{id: 111, value: 111}, {id: 112, value: 112}, {id: 113, value: 113}, {id: 114, value: 114}, {id: 115, value: 115},
			{id: 116, value: 116}, {id: 117, value: 117}, {id: 118, value: 118}, {id: 119, value: 119}, {id: 120, value: 120}
		] insert test.source"#,
		Params::None,
	)
	.await
	.unwrap();

	sleep(Duration::from_millis(10)).await;

	for frame in db.query_as_root("from test.even_numbers take 10", Params::None).await.unwrap() {
		println!("{}", frame);
	}

	for frame in db
		.query_as_root("from test.even_numbers aggregate { count: math::count(value) }", Params::None)
		.await
		.unwrap()
	{
		println!("{}", frame);
	}
}
