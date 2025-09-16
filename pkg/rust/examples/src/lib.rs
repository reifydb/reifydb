use reifydb::log_info;

/// Helper function to log queries with formatting
/// The query text is displayed in bold for better readability
pub fn log_query(query: &str) {
	log_info!("Query:");
	let formatted_query =
		query.lines().map(|line| format!("\x1b[1m{}\x1b[0m", line)).collect::<Vec<_>>().join("\n");
	log_info!("{}", formatted_query);
}
