import React from 'react';
import { 
    ConnectionProvider, 
    useCommandOne, 
    useCommandMany,
    useQueryOne,
    Schema 
} from '@reifydb/react';

// Example: Creating a user management component
function UserManager() {
    // Schema for user data
    const userSchema = Schema.object({
        id: Schema.number(),
        name: Schema.string(),
        email: Schema.string(),
        created: Schema.string()
    });

    // Query to get all users
    const { result: users, error: queryError } = useQueryOne(
        `FROM users SELECT *`,
        undefined,
        Schema.array(userSchema)
    );

    // Command to add a new user
    const { 
        result: addResult, 
        error: addError, 
        isExecuting: isAdding 
    } = useCommandOne(
        `INSERT INTO users VALUES {name: :name, email: :email, created: CURRENT_TIMESTAMP}`,
        { name: 'New User', email: 'user@example.com' },
        userSchema
    );

    // Command to delete a user
    const { 
        result: deleteResult, 
        error: deleteError,
        isExecuting: isDeleting 
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
            {isAdding && <p>Adding user...</p>}
            {isDeleting && <p>Deleting user...</p>}
            
            {/* Show errors */}
            {queryError && <p>Query error: {queryError}</p>}
            {addError && <p>Add error: {addError}</p>}
            {deleteError && <p>Delete error: {deleteError}</p>}
            
            {/* Show affected rows */}
            {addResult?.rowsAffected && <p>Added {addResult.rowsAffected} user(s)</p>}
            {deleteResult?.rowsAffected && <p>Deleted {deleteResult.rowsAffected} user(s)</p>}
        </div>
    );
}

// Example: Batch operations with multiple commands
function DatabaseSetup() {
    const { 
        results, 
        error, 
        isExecuting 
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

    if (isExecuting) return <p>Setting up database...</p>;
    if (error) return <p>Setup error: {error}</p>;
    
    return (
        <div>
            <h2>Database Setup Complete</h2>
            <p>Executed {results?.length} commands</p>
            {results?.map((result, i) => (
                <div key={i}>
                    Command {i + 1}: {result.rowsAffected ?? 0} rows affected
                </div>
            ))}
        </div>
    );
}

// Example: Using with custom connection config
function CustomConnectionExample() {
    const customConfig = {
        url: 'ws://localhost:8091',
        options: { timeoutMs: 5000 }
    };

    const { result, error } = useCommandOne(
        `UPDATE settings SET value = :value WHERE key = :key`,
        { key: 'theme', value: 'dark' },
        undefined,
        { connectionConfig: customConfig }
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