// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	Params, Session, WithSubsystem,
	core::{interface::logging::LogLevel::Info, util::clock},
	embedded,
	sub_logging::{FormatStyle, LoggingBuilder},
};

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true).format_style(FormatStyle::Timeline))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(Info)
}

fn main() {
	// Set mock time to a known value for testing time-based windows
	let base_time = 1000000; // Start at 1,000,000 milliseconds
	clock::mock_time_set(base_time);

	let mut db =
		embedded::memory_optimistic().with_logging(logger_configuration).with_worker(|wp| wp).build().unwrap();

	db.start().unwrap();

	// =================
	// WINDOW FUNCTIONALITY TESTING
	// =================
	match db.command_as_root(r#"CREATE NAMESPACE iot"#, Params::None) {
		Ok(_) => println!("✅ IoT namespace created"),
		Err(e) => println!("❌ Namespace creation failed: {}", e),
	}

	match db.command_as_root(
		r#"CREATE TABLE iot.sensors { 
			sensor_id: UTF8, 
			location: UTF8, 
			temperature: FLOAT8, 
			humidity: FLOAT8, 
			pressure: FLOAT8,
			timestamp: INT8 
		}"#,
		Params::None,
	) {
		Ok(_) => println!("✅ Sensors table created"),
		Err(e) => println!("❌ Table creation failed: {}", e),
	}

	println!("\n🪟 Creating window-based analytics views...");

	// 1. TIME-BASED WINDOW: 5-minute temperature averages per location
	println!("\n1️⃣ Time-based Window (5-minute intervals):");
	match db.command_as_root(
		r#"CREATE DEFERRED VIEW iot.temp_5min { 
		   location: UTF8,
		   avg_temperature: FLOAT8,
		   sum_humidity: FLOAT8 
		} AS {
		   FROM iot.sensors 
		   WINDOW { avg_temperature: avg(temperature), sum_humidity: sum(humidity) } 
		   WITH { interval: "100ms" } 
		   BY { location }
		}"#,
		Params::None,
	) {
		Ok(_) => println!("   ✅ 5-minute temperature averages view created"),
		Err(e) => println!("   ❌ Time window view failed: {}", e),
	}

	// 3. SLIDING WINDOW: 1-hour window sliding every 10 minutes
	println!("\n3️⃣ Sliding Window (1-hour window, 10-minute slide):");
	match db.command_as_root(
		r#"CREATE DEFERRED VIEW iot.sliding_temp {
		   location: UTF8,
		   sensor_id: UTF8,
		   avg_temperature: FLOAT8,
		   sum_humidity: FLOAT8
		} AS {
		   FROM iot.sensors 
		   WINDOW { avg_temperature: avg(temperature), sum_humidity: sum(humidity) } 
		   WITH { interval: "2s", slide: "1s" } 
		   BY { location, sensor_id }
		}"#,
		Params::None,
	) {
		Ok(_) => println!("   ✅ Sliding window view created"),
		Err(e) => println!("   ❌ Sliding window view failed: {}", e),
	}

	// 4. MULTIPLE AGGREGATIONS with flexible syntax (BY before WITH)
	println!("\n4️⃣ Multi-aggregation Window (flexible BY/WITH syntax):");
	match db.command_as_root(
		r#"CREATE DEFERRED VIEW iot.sensor_stats {
		   sensor_id: UTF8,
		   location: UTF8,
		   avg_temperature: FLOAT8,
		   sum_humidity: FLOAT8,
		   avg_pressure: FLOAT8
		} AS {
		   FROM iot.sensors
		   WINDOW {
			   avg_temperature: avg(temperature),
			   sum_humidity: sum(humidity),
			   avg_pressure: avg(pressure)
		   }
		   BY { sensor_id, location }
		   WITH { interval: "100ms" }
		}"#,
		Params::None,
	) {
		Ok(_) => println!("   ✅ Multi-aggregation window view created"),
		Err(e) => println!("   ❌ Multi-agg window view failed: {}", e),
	}

	// 📊 INSERT TEST DATA FOR WINDOW VALIDATION
	println!("\n📊 Inserting test sensor data for window validation...");

	// Insert realistic IoT sensor data using proper ReifyDB syntax with current timestamps
	let current_time = clock::now_millis();

	match db.command_as_root(
		&format!(r#"FROM [
			{{ sensor_id: "sensor_a", location: "kitchen", temperature: 22.5, humidity: 45.0, pressure: 1013.2, timestamp: {} }},
			{{ sensor_id: "sensor_a", location: "kitchen", temperature: 23.1, humidity: 46.5, pressure: 1013.1, timestamp: {} }},
			{{ sensor_id: "sensor_a", location: "kitchen", temperature: 23.8, humidity: 48.0, pressure: 1012.9, timestamp: {} }},
			{{ sensor_id: "sensor_b", location: "living_room", temperature: 21.2, humidity: 42.0, pressure: 1014.1, timestamp: {} }},
			{{ sensor_id: "sensor_b", location: "living_room", temperature: 21.8, humidity: 43.5, pressure: 1014.0, timestamp: {} }},
			{{ sensor_id: "sensor_b", location: "living_room", temperature: 22.4, humidity: 44.8, pressure: 1013.8, timestamp: {} }},
			{{ sensor_id: "sensor_c", location: "bedroom", temperature: 20.5, humidity: 50.0, pressure: 1012.5, timestamp: {} }},
			{{ sensor_id: "sensor_c", location: "bedroom", temperature: 20.9, humidity: 51.2, pressure: 1012.4, timestamp: {} }},
			{{ sensor_id: "sensor_c", location: "bedroom", temperature: 21.3, humidity: 52.5, pressure: 1012.2, timestamp: {} }}
		]
		INSERT iot.sensors"#, 
			current_time, current_time + 1000, current_time + 2000,
			current_time + 500, current_time + 1500, current_time + 2500,
			current_time + 200, current_time + 1200, current_time + 2200
		),
		Params::None,
	) {
		Ok(_) => println!("Initial sensor readings inserted successfully"),
		Err(e) => println!("Initial insert failed: {}", e),
	}

	// Wait for windows to potentially trigger (using real time since windows use real timestamps)
	println!("Waiting 2 seconds for 1-second windows to trigger...");
	sleep(Duration::from_secs(2));

	// 🔍 QUERY WINDOW VIEWS TO VALIDATE FUNCTIONALITY
	println!("\n🔍 Querying window views to validate functionality...");

	// Query 1: Time-based window (5-minute intervals)
	println!("\n1️⃣ Time-based Window Results (iot.temp_5min):");
	match db.command_as_root(r#"FROM iot.temp_5min"#, Params::None) {
		Ok(frames) => {
			println!("   📊 {} result rows:", frames.len());
			for frame in frames {
				println!("   {}", frame);
			}
		}
		Err(e) => println!("   ❌ Query failed: {}", e),
	}

	// Query 3: Sliding window (1-hour sliding every 10 minutes)
	println!("\n3️⃣ Sliding Window Results (iot.sliding_temp):");
	match db.command_as_root(r#"FROM iot.sliding_temp"#, Params::None) {
		Ok(frames) => {
			println!("   📊 {} result rows:", frames.len());
			for frame in frames {
				println!("   {}", frame);
			}
		}
		Err(e) => println!("   ❌ Query failed: {}", e),
	}

	// Query 4: Multi-aggregation window (15-minute intervals)
	println!("\n4️⃣ Multi-aggregation Window Results (iot.sensor_stats):");
	match db.command_as_root(r#"FROM iot.sensor_stats"#, Params::None) {
		Ok(frames) => {
			println!("   📊 {} result rows:", frames.len());
			for frame in frames {
				println!("   {}", frame);
			}
		}
		Err(e) => println!("   ❌ Query failed: {}", e),
	}

	// =================
	// VARIABLE FUNCTIONALITY TESTING
	// =================
	println!("\n\n=== VARIABLE TESTING ===");

	// Test variable shadowing (should work)
	println!("=== Testing Shadowing ===");
	for frame in db
		.command_as_root(
			r#"
		let $x := 10; 
		let $x := 20; 
		MAP { $x }
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test mutable assignment (should work)
	println!("=== Testing Mutable Assignment ===");
	for frame in db
		.command_as_root(
			r#"
		let mut $x := 10; 
		$x := 20; 
		MAP { $x }
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test immutable assignment (should fail)
	println!("=== Testing Immutable Assignment (should fail) ===");
	match db.command_as_root(
		r#"
		let $x := 10; 
		$x := 20; 
		MAP { $x }
	"#,
		Params::None,
	) {
		Ok(_) => println!("ERROR: Should have failed!"),
		Err(e) => println!("✓ Correctly failed: {}", e),
	}

	sleep(Duration::from_millis(100));
}
