// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import React from 'react';
import { 
    ConnectionProvider, 
    useCommandOne, 
    useCommandMany,
    useQueryOne,
    Shape 
} from '@reifydb/react';

// Example: Creating a user management component
function UserManager() {
    // Shape for user data
    const user_shape = Shape.object({
        id: Shape.number(),
        name: Shape.string(),
        email: Shape.string(),
        created: Shape.string()
    });

    // Query to get all users
    const { result: users, error: query_error } = useQueryOne(
        `FROM users SELECT *`,
        undefined,
        Shape.array(user_shape)
    );

    // Command to add a new user
    const { 
        result: add_result, 
        error: add_error, 
        is_executing: is_adding 
    } = useCommandOne(
        `INSERT INTO users VALUES {name: :name, email: :email, created: CURRENT_TIMESTAMP}`,
        { name: 'New User', email: 'user@example.com' },
        user_shape
    );

    // Command to delete a user
    const { 
        result: delete_result, 
        error: delete_error,
        is_executing: is_deleting 
    } = useCommandOne(
        `DELETE FROM users WHERE id = :id`,
        { id: 1 }
    );

    return (
        <div>
            <h2>User Management</h2>
            
            {/* Display users */}
            {users?.rows?.map(user => (
                <div key={user.id}>
                    {user.name} - {user.email}
                </div>
            ))}

            {/* Show command status */}
            {is_adding && <p>Adding user...</p>}
            {is_deleting && <p>Deleting user...</p>}
            
            {/* Show errors */}
            {query_error && <p>Query error: {query_error}</p>}
            {add_error && <p>Add error: {add_error}</p>}
            {delete_error && <p>Delete error: {delete_error}</p>}
            
            {/* Show affected rows */}
            {add_result?.rows_affected && <p>Added {add_result.rows_affected} user(s)</p>}
            {delete_result?.rows_affected && <p>Deleted {delete_result.rows_affected} user(s)</p>}
        </div>
    );
}

// Example: Batch operations with multiple commands
function DatabaseSetup() {
    const { 
        results, 
        error, 
        is_executing 
    } = useCommandMany([
        `CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            created TEXT
        )`,
        `CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)`,
        `INSERT INTO users VALUES {id: 1, name: 'Admin', email: 'admin@example.com', created: CURRENT_TIMESTAMP}`
    ]);

    if (is_executing) return <p>Setting up database...</p>;
    if (error) return <p>Setup error: {error}</p>;
    
    return (
        <div>
            <h2>Database Setup Complete</h2>
            <p>Executed {results?.length} commands</p>
            {results?.map((result, i) => (
                <div key={i}>
                    Command {i + 1}: {result.rows_affected ?? 0} rows affected
                </div>
            ))}
        </div>
    );
}

// Example: Using with custom connection config
function CustomConnectionExample() {
    const custom_config = {
        url: 'ws://localhost:8091',
        options: { timeout_ms: 5000 }
    };

    const { result, error } = useCommandOne(
        `UPDATE settings SET value = :value WHERE key = :key`,
        { key: 'theme', value: 'dark' },
        undefined,
        { connection_config: custom_config }
    );

    return (
        <div>
            {error ? <p>Error: {error}</p> : <p>Setting updated</p>}
        </div>
    );
}

// Main App with ConnectionProvider
export default function App() {
    return (
        <ConnectionProvider config={{ url: 'ws://localhost:8090' }}>
            <UserManager />
            <DatabaseSetup />
            <CustomConnectionExample />
        </ConnectionProvider>
    );
}