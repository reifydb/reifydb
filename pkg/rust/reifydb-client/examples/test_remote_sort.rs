// Test program to connect to remote ReifyDB server and test sorting
use reifydb_client::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("Connecting to ws://192.168.100.52:8090/\n");

	// Connect to the remote server
	let client = Client::ws(("192.168.100.52", 8090))?;

	// Try with auth token from environment or use "root"
	let token = std::env::var("REIFYDB_TOKEN").unwrap_or_else(|_| "root".to_string());
	println!(
		"Using auth token: {}\n",
		if token == "root" {
			"root (default)"
		} else {
			&token
		}
	);

	// Create a blocking session with auth token
	let mut session = client.blocking_session(Some(token))?;

	println!("Connected successfully!\n");

	// Test ASC
	println!("=== TEST 1: ASC (should show smallest first) ===");
	println!("Query:");
	println!("from system.table_storage_stats");
	println!("sort total_bytes asc\n");

	let query_asc = "from system.table_storage_stats\nsort total_bytes asc";
	let result_asc = session.query(query_asc, None)?;

	if let Some(frame) = result_asc.frames.first() {
		if let Some(total_bytes_col) = frame.columns.iter().find(|c| c.name == "total_bytes") {
			let mut values: Vec<u64> = Vec::new();
			for i in 0..total_bytes_col.data.len() {
				let val = total_bytes_col.data.as_string(i).parse::<u64>().unwrap_or(0);
				values.push(val);
			}
			println!("ASC Results: {:?}", values);
			println!("First value (should be smallest): {}", values[0]);
			println!("Last value (should be largest): {}\n", values[values.len() - 1]);
		}
	}

	// Test DESC
	println!("=== TEST 2: DESC (should show largest first) ===");
	println!("Query:");
	println!("from system.table_storage_stats");
	println!("sort total_bytes desc\n");

	let query = "from system.table_storage_stats\nsort total_bytes desc";
	let result = session.query(query, None)?;

	println!("Query executed: {} frames returned\n", result.frames.len());

	// Print the results
	if let Some(frame) = result.frames.first() {
		println!("Frame output:");
		println!("{}\n", frame);

		// Also analyze the data
		if let Some(total_bytes_col) = frame.columns.iter().find(|c| c.name == "total_bytes") {
			println!("=== Analyzing total_bytes column ===");
			let mut values: Vec<u64> = Vec::new();
			for i in 0..total_bytes_col.data.len() {
				let val = total_bytes_col.data.as_string(i).parse::<u64>().unwrap_or(0);
				values.push(val);
				println!("Row {}: {} bytes", i, val);
			}

			println!("\nValues in order: {:?}", values);

			// Check if sorted correctly (DESC = largest first)
			let mut is_desc_sorted = true;
			for i in 1..values.len() {
				if values[i - 1] < values[i] {
					is_desc_sorted = false;
					println!(
						"\n⚠️  SORTING ERROR at position {}: {} < {}",
						i,
						values[i - 1],
						values[i]
					);
				}
			}

			if is_desc_sorted {
				println!("\n✅ Correctly sorted in DESCENDING order (largest first)");
			} else {
				println!("\n❌ NOT correctly sorted in descending order!");
				println!("   Expected: Largest value first, decreasing values");
				println!("   Got: {:?}", values);
			}
		}
	}

	Ok(())
}
