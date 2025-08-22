// //! Shared utilities for ReifyDB examples
//
// pub mod utils {
//     use anyhow::Result;
//     use tracing_subscriber;
//
//     /// Initialize logging for examples
//     pub fn setup_logging() {
//         let _ = tracing_subscriber::fmt()
//             .with_env_filter(
//                 tracing_subscriber::EnvFilter::try_from_default_env()
//                     .unwrap_or_else(|_|
// tracing_subscriber::EnvFilter::new("info"))             )
//             .try_init();
//     }
//
//     /// Print a section separator with title
//     pub fn print_section(title: &str) {
//         println!("\n{}", "=".repeat(60));
//         println!("{}", title);
//         println!("{}\n", "=".repeat(60));
//     }
//
//     /// Print a subsection with smaller separator
//     pub fn print_subsection(title: &str) {
//         println!("\n{}", "-".repeat(40));
//         println!("{}", title);
//         println!("{}", "-".repeat(40));
//     }
//
//     /// Print a result in a formatted way
//     pub fn print_result<T: std::fmt::Debug>(label: &str, result: &T) {
//         println!("{}: {:?}", label, result);
//     }
//
//     /// Create a temporary in-memory database for testing
//     pub async fn create_temp_db() -> Result<reifydb::Database> {
//         use reifydb::presets::Presets;
//
//         let db = reifydb::Database::builder()
//             .preset(Presets::Memory)
//             .build()
//             .await?;
//
//         Ok(db)
//     }
//
//     /// Create a temporary SQLite database for testing
//     #[cfg(feature = "storage-sqlite")]
//     pub async fn create_sqlite_db(path: Option<&str>) ->
// Result<reifydb::Database> {         use reifydb::presets::Presets;
//
//         let db = if let Some(path) = path {
//             reifydb::Database::builder()
//                 .preset(Presets::Sqlite(path.into()))
//                 .build()
//                 .await?
//         } else {
//             // Use temporary file
//             let temp_dir = std::env::temp_dir();
//             let db_path = temp_dir.join(format!("reifydb_example_{}.db",
// uuid::Uuid::new_v4()));             reifydb::Database::builder()
//                 .preset(Presets::Sqlite(db_path))
//                 .build()
//                 .await?
//         };
//
//         Ok(db)
//     }
//
//     /// Helper to display query results in a table format
//     pub fn display_results(results: &reifydb::ResultSet) {
//         if results.is_empty() {
//             println!("No results found.");
//             return;
//         }
//
//         // Get column names
//         let columns = results.columns();
//
//         // Print header
//         for (i, col) in columns.iter().enumerate() {
//             if i > 0 {
//                 print!(" | ");
//             }
//             print!("{:15}", col.name());
//         }
//         println!();
//         println!("{}", "-".repeat(columns.len() * 17));
//
//         // Print rows
//         for row in results.rows() {
//             for (i, col) in columns.iter().enumerate() {
//                 if i > 0 {
//                     print!(" | ");
//                 }
//                 let value =
// row.get(col.name()).unwrap_or(&reifydb::Value::Null);
// print!("{:15}", format_value(value));             }
//             println!();
//         }
//
//         println!("\nTotal rows: {}", results.len());
//     }
//
//     fn format_value(value: &reifydb::Value) -> String {
//         match value {
//             reifydb::Value::Null => "NULL".to_string(),
//             reifydb::Value::Bool(b) => b.to_string(),
//             reifydb::Value::Int(i) => i.to_string(),
//             reifydb::Value::Float(f) => format!("{:.2}", f),
//             reifydb::Value::Text(s) => {
//                 if s.len() > 12 {
//                     format!("{}...", &s[..12])
//                 } else {
//                     s.clone()
//                 }
//             }
//             reifydb::Value::Blob(b) => format!("<blob:{}>", b.len()),
//             reifydb::Value::Date(d) => d.to_string(),
//             reifydb::Value::DateTime(dt) => dt.to_string(),
//             reifydb::Value::Time(t) => t.to_string(),
//             reifydb::Value::Uuid(u) => u.to_string()[..8].to_string(),
//         }
//     }
//
//     /// Helper to create sample data
//     pub mod sample_data {
//         use anyhow::Result;
//
//         /// Create a sample users table
//         pub async fn create_users_table(db: &reifydb::Database) -> Result<()>
// {             db.execute(
//                 "CREATE TABLE users (
//                     id INTEGER PRIMARY KEY,
//                     name TEXT NOT NULL,
//                     email TEXT UNIQUE NOT NULL,
//                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//                 )"
//             ).await?;
//
//             Ok(())
//         }
//
//         /// Create a sample products table
//         pub async fn create_products_table(db: &reifydb::Database) ->
// Result<()> {             db.execute(
//                 "CREATE TABLE products (
//                     id INTEGER PRIMARY KEY,
//                     name TEXT NOT NULL,
//                     price DECIMAL(10,2) NOT NULL,
//                     stock INTEGER DEFAULT 0,
//                     category TEXT
//                 )"
//             ).await?;
//
//             Ok(())
//         }
//
//         /// Create a sample orders table
//         pub async fn create_orders_table(db: &reifydb::Database) ->
// Result<()> {             db.execute(
//                 "CREATE TABLE orders (
//                     id INTEGER PRIMARY KEY,
//                     user_id INTEGER REFERENCES users(id),
//                     product_id INTEGER REFERENCES products(id),
//                     quantity INTEGER NOT NULL,
//                     order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//                 )"
//             ).await?;
//
//             Ok(())
//         }
//
//         /// Insert sample users
//         pub async fn insert_sample_users(db: &reifydb::Database) ->
// Result<()> {             let users = vec![
//                 ("Alice Johnson", "alice@example.com"),
//                 ("Bob Smith", "bob@example.com"),
//                 ("Charlie Brown", "charlie@example.com"),
//                 ("Diana Prince", "diana@example.com"),
//                 ("Eve Wilson", "eve@example.com"),
//             ];
//
//             for (name, email) in users {
//                 db.execute_params(
//                     "INSERT INTO users (name, email) VALUES (?, ?)",
//                     &[
//                         reifydb::Value::Text(name.to_string()),
//                         reifydb::Value::Text(email.to_string()),
//                     ]
//                 ).await?;
//             }
//
//             Ok(())
//         }
//
//         /// Insert sample products
//         pub async fn insert_sample_products(db: &reifydb::Database) ->
// Result<()> {             let products = vec![
//                 ("Laptop", 999.99, 10, "Electronics"),
//                 ("Mouse", 29.99, 50, "Electronics"),
//                 ("Keyboard", 79.99, 30, "Electronics"),
//                 ("Monitor", 299.99, 15, "Electronics"),
//                 ("Desk Chair", 199.99, 20, "Furniture"),
//                 ("Standing Desk", 499.99, 8, "Furniture"),
//                 ("Notebook", 4.99, 100, "Stationery"),
//                 ("Pen Set", 12.99, 75, "Stationery"),
//             ];
//
//             for (name, price, stock, category) in products {
//                 db.execute_params(
//                     "INSERT INTO products (name, price, stock, category)
// VALUES (?, ?, ?, ?)",                     &[
//                         reifydb::Value::Text(name.to_string()),
//                         reifydb::Value::Float(price),
//                         reifydb::Value::Int(stock),
//                         reifydb::Value::Text(category.to_string()),
//                     ]
//                 ).await?;
//             }
//
//             Ok(())
//         }
//     }
//
//     /// Performance measurement utilities
//     pub mod perf {
//         use std::time::Instant;
//
//         pub struct Timer {
//             start: Instant,
//             label: String,
//         }
//
//         impl Timer {
//             pub fn new(label: &str) -> Self {
//                 println!("Starting: {}", label);
//                 Self {
//                     start: Instant::now(),
//                     label: label.to_string(),
//                 }
//             }
//         }
//
//         impl Drop for Timer {
//             fn drop(&mut self) {
//                 let elapsed = self.start.elapsed();
//                 println!("Completed: {} in {:?}", self.label, elapsed);
//             }
//         }
//     }
// }
