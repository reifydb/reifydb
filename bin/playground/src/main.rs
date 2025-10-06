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
	println!("DEBUG: Set mock time to {}", base_time);

	let mut db =
		embedded::memory_optimistic().with_logging(logger_configuration).with_worker(|wp| wp).build().unwrap();

	db.start().unwrap();

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
		   WITH { interval: "5m" } 
		   BY { location }
		}"#,
		Params::None,
	) {
		Ok(_) => println!("   ✅ 5-minute temperature averages view created"),
		Err(e) => println!("   ❌ Time window view failed: {}", e),
	}

	// // 2. COUNT-BASED WINDOW: Every 100 readings statistics
	// println!("\n2️⃣ Count-based Window (every 100 readings):");
	// match db.command_as_root(
	// 	r#"CREATE DEFERRED VIEW iot.readings_100 {
	// 	   sensor_id: UTF8,
	// 	   avg_temperature: FLOAT8,
	// 	   max_humidity: FLOAT8
	// 	} AS {
	// 	   FROM iot.sensors
	// 	   WINDOW { avg_temperature: avg(temperature), max_humidity: max(humidity) }
	// 	   WITH { count: 100 }
	// 	   BY { sensor_id }
	// 	}"#,
	// 	Params::None,
	// ) {
	// 	Ok(_) => println!("   ✅ 100-reading statistics view created"),
	// 	Err(e) => println!("   ❌ Count window view failed: {}", e),
	// }

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
		   WITH { interval: "1h", slide: "10m" } 
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
		   WITH { interval: "15m" }
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
	println!("DEBUG: Current time for initial data: {}", current_time);

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

	// Advance mock time to trigger time-based windows
	clock::mock_time_advance(6 * 60 * 1000); // Advance by 6 minutes to trigger 5-minute windows
	let new_time = clock::now_millis();
	println!("DEBUG: Advanced time to {} (advanced by 6 minutes)", new_time);

	// Insert additional data for count-based window testing (need >100 readings)
	println!("\nInserting additional data for count-based window testing...");

	// Create a batch of 100 additional readings with current timestamps
	let mut additional_data = Vec::new();
	let batch_start_time = clock::now_millis();
	for i in 10..120 {
		let timestamp = batch_start_time + (i * 100);
		let temp = 20.0 + (i % 10) as f64 * 0.5;
		let humidity = 40.0 + (i % 15) as f64 * 2.0;
		let pressure = 1012.0 + (i % 20) as f64 * 0.1;

		additional_data.push(format!(
			r#"{{ sensor_id: "sensor_a", location: "kitchen", temperature: {}, humidity: {}, pressure: {}, timestamp: {} }}"#,
			temp, humidity, pressure, timestamp
		));
	}

	let batch_query = format!(r#"FROM [{}] INSERT iot.sensors"#, additional_data.join(",\n\t\t\t"));

	match db.command_as_root(&batch_query, Params::None) {
		Ok(_) => println!("Added {} additional readings for count-based testing", additional_data.len()),
		Err(e) => println!("Batch insert failed: {}", e),
	}

	// Advance time again to trigger more windows
	clock::mock_time_advance(10 * 60 * 1000); // Advance by another 10 minutes
	let final_time = clock::now_millis();
	println!("DEBUG: Final time advanced to {} (total 16 minutes from start)", final_time);

	sleep(Duration::from_millis(100));

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

	// Query 2: Count-based window (every 100 readings)
	// println!("\n2️⃣ Count-based Window Results (iot.readings_100):");
	// match db.command_as_root(r#"FROM iot.readings_100"#, Params::None) {
	// 	Ok(frames) => {
	// 		println!("   📊 {} result rows:", frames.len());
	// 		for frame in frames {
	// 			println!("   {}", frame);
	// 		}
	// 	}
	// 	Err(e) => println!("   ❌ Query failed: {}", e),
	// }

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
}
