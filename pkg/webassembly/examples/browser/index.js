// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Import the WASM module
// Update this path to match your build output
import init, { WasmDB } from '../../dist/web/reifydb_webassembly.js';

let db = null;
let queryCount = 0;
let totalRows = 0;
let lastQueryTime = 0;

// Example queries
const EXAMPLES = {
    basic: `-- Basic query example
FROM [
  { name: "Alice", age: 30, city: "NYC" },
  { name: "Bob", age: 25, city: "LA" },
  { name: "Carol", age: 35, city: "NYC" }
]
MAP { name, age }`,

    'create-table': `-- Create a table and insert data
CREATE NAMESPACE demo;

CREATE TABLE demo.users {
  id: int4,
  name: utf8,
  age: int4,
  city: utf8
};

INSERT demo.users FROM [
  { id: 1, name: "Alice", age: 30, city: "NYC" },
  { id: 2, name: "Bob", age: 25, city: "LA" },
  { id: 3, name: "Carol", age: 35, city: "NYC" }
];

FROM demo.users;`,

    filter: `-- Filtering data
FROM [
  { product: "Laptop", price: 1200, category: "Electronics" },
  { product: "Mouse", price: 25, category: "Electronics" },
  { product: "Desk", price: 350, category: "Furniture" },
  { product: "Chair", price: 200, category: "Furniture" }
]
FILTER price > 100
MAP { product, price }`,

    aggregate: `-- Aggregation example
FROM [
  { name: "Alice", department: "Engineering", salary: 100000 },
  { name: "Bob", department: "Engineering", salary: 95000 },
  { name: "Carol", department: "Sales", salary: 85000 },
  { name: "David", department: "Sales", salary: 90000 }
]
AGGREGATE {
  total_employees: count(),
  avg_salary: avg(salary),
  max_salary: max(salary),
  min_salary: min(salary)
}`,

    'group-by': `-- Group by example
FROM [
  { name: "Alice", department: "Engineering", salary: 100000 },
  { name: "Bob", department: "Engineering", salary: 95000 },
  { name: "Carol", department: "Sales", salary: 85000 },
  { name: "David", department: "Sales", salary: 90000 }
]
GROUP BY department
AGGREGATE {
  department,
  count: count(),
  avg_salary: avg(salary)
}`,

    join: `-- Create two tables and join them
CREATE NAMESPACE demo;

CREATE TABLE demo.users {
  id: int4,
  name: utf8
};

CREATE TABLE demo.orders {
  id: int4,
  user_id: int4,
  total: int4
};

INSERT demo.users FROM [
  { id: 1, name: "Alice" },
  { id: 2, name: "Bob" }
];

INSERT demo.orders FROM [
  { id: 1, user_id: 1, total: 150 },
  { id: 2, user_id: 1, total: 200 },
  { id: 3, user_id: 2, total: 100 }
];

FROM demo.orders
JOIN demo.users ON orders.user_id = users.id
MAP { order_id: orders.id, user_name: users.name, total: orders.total };`
};

// Initialize the WASM database
async function initializeDb() {
    try {
        updateStatus('Initializing WASM...');
        await init();

        updateStatus('Creating database...');
        db = new WasmDB();
        window.db = db;

        updateStatus('Ready ✓');
        document.getElementById('run-query').disabled = false;

        console.log('ReifyDB WASM database initialized successfully!');
    } catch (error) {
        updateStatus('Failed to initialize');
        showError('Failed to initialize WASM database: ' + error.message);
        console.error(error);
    }
}

// Run query
async function runCommand() {
    if (!db) {
        showError('Database not initialized');
        return;
    }

    const query = document.getElementById('query-editor').value.trim();
    if (!query) {
        showError('Please enter a query');
        return;
    }

    const resultsContainer = document.getElementById('results-container');
    resultsContainer.innerHTML = '<div class="loading"><div class="spinner"></div>Executing query...</div>';

    const startTime = performance.now();

    try {
        let results = await db.admin(query);

        const endTime = performance.now();
        lastQueryTime = Math.round(endTime - startTime);

        queryCount++;
        totalRows += results.length;

        updateStats();
        displayResults(results, lastQueryTime);

    } catch (error) {
        const endTime = performance.now();
        lastQueryTime = Math.round(endTime - startTime);
        updateStats();
        showError('Query failed: ' + error);
        console.error(error);
    }
}

// Display results in table format
function displayResults(results, executionTime) {
    const resultsContainer = document.getElementById('results-container');
    const resultCount = document.getElementById('result-count');

    if (!results || results.length === 0) {
        resultsContainer.innerHTML = '<div class="info">Query executed successfully. No rows returned.</div>';
        resultCount.textContent = '';
        return;
    }

    resultCount.textContent = `${results.length} row${results.length !== 1 ? 's' : ''}`;

    // Get column names from first row
    const columns = Object.keys(results[0]);

    // Build table HTML
    let html = '<table class="results-table"><thead><tr>';
    columns.forEach(col => {
        html += `<th>${escapeHtml(col)}</th>`;
    });
    html += '</tr></thead><tbody>';

    results.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            const value = row[col];
            html += `<td>${formatValue(value)}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table>';
    html += `<div class="success">✓ Query executed in ${executionTime}ms</div>`;

    resultsContainer.innerHTML = html;
}

// Show error message
function showError(message) {
    const resultsContainer = document.getElementById('results-container');
    resultsContainer.innerHTML = `<div class="error">❌ ${escapeHtml(message)}</div>`;
    document.getElementById('result-count').textContent = '';
}

// Update status indicator
function updateStatus(status) {
    document.getElementById('status').textContent = status;
}

// Update statistics
function updateStats() {
    document.getElementById('stat-queries').textContent = queryCount;
    document.getElementById('stat-rows').textContent = totalRows;
    document.getElementById('stat-time').textContent = lastQueryTime + 'ms';
}

// Format value for display
function formatValue(value) {
    if (value === null || value === undefined) {
        return '<em>null</em>';
    }
    if (typeof value === 'object') {
        return escapeHtml(JSON.stringify(value));
    }
    return escapeHtml(String(value));
}

// Escape HTML
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Event listeners
document.getElementById('run-query').addEventListener('click', runCommand);

document.getElementById('clear-query').addEventListener('click', () => {
    document.getElementById('query-editor').value = '';
    document.getElementById('results-container').innerHTML = '<div class="info">Query cleared. Ready for new input.</div>';
    document.getElementById('result-count').textContent = '';
});

document.getElementById('example-select').addEventListener('change', (e) => {
    const example = e.target.value;
    if (example && EXAMPLES[example]) {
        document.getElementById('query-editor').value = EXAMPLES[example];
    }
});

// Allow Ctrl+Enter to run query
document.getElementById('query-editor').addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        runCommand();
    }
});

// Initialize on page load
initializeDb();
