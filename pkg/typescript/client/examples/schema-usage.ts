/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

/**
 * Schema Integration Usage Examples
 * 
 * This file demonstrates how to use the schema system with ReifyDB client
 * for type-safe parameter encoding and result decoding.
 */

import {
    Client,
    Schema,
    BidirectionalSchema
} from '@reifydb/client';

// Example usage (this would run in an actual application)
async function schemaExamples() {
    // Connect to ReifyDB
    const client = await Client.connect_ws('ws://localhost:8080/ws');

    // Example 1: Simple primitive schema
    const userQuerySchema: BidirectionalSchema = {
        params: Schema.object({
            userId: Schema.number(),
            includeInactive: Schema.boolean()
        }),
        result: Schema.object({
            id: Schema.number(),
            name: Schema.string(),
            email: Schema.string(),
            isActive: Schema.boolean()
        })
    };

    // TypeScript knows the exact types!
    const users = await client.queryWithSchema<{
        id: number;
        name: string; 
        email: string;
        isActive: boolean;
    }>(
        'MAP { $userId as id, "John Doe" as name, "john@example.com" as email, $includeInactive as isActive }',
        {
            userId: 123,
            includeInactive: false
        },
        userQuerySchema
    );

    // users[0][0].id is type `number`, not a Value object!
    console.log('User ID:', users[0][0].id);
    console.log('User name:', users[0][0].name);

    // Example 2: Array parameters
    const arraySchema: BidirectionalSchema = {
        params: Schema.array(Schema.string()),
        result: Schema.object({
            item1: Schema.string(),
            item2: Schema.string(),
            item3: Schema.string()
        })
    };

    const arrayResult = await client.commandWithSchema<{
        item1: string;
        item2: string;
        item3: string;
    }>(
        'MAP { $1 as item1, $2 as item2, $3 as item3 }',
        ['apple', 'banana', 'cherry'],
        arraySchema
    );

    console.log('Items:', arrayResult[0][0]);

    // Example 3: Mixed types with auto-detection
    const mixedSchema: BidirectionalSchema = {
        params: Schema.object({
            id: Schema.number(),
            name: Schema.string(),
            active: Schema.boolean(),
            metadata: Schema.auto() // Auto-detect type
        }),
        result: Schema.object({
            id_field: Schema.number(),
            name_field: Schema.string(),
            status: Schema.boolean(),
            meta_str: Schema.string()
        })
    };

    const mixedResult = await client.queryWithSchema<{
        id_field: number;
        name_field: string;
        status: boolean;
        meta_str: string;
    }>(
        'MAP { $id as id_field, $name as name_field, $active as status, CAST($metadata as TEXT) as meta_str }',
        {
            id: 456,
            name: 'Jane Smith',
            active: true,
            metadata: 'some data' // Auto-detected as string
        },
        mixedSchema
    );

    console.log('Mixed result:', mixedResult[0][0]);

    // Example 4: Compare with traditional approach
    console.log('\n=== Comparison: Schema vs Traditional ===');

    // Traditional approach (returns Value objects)
    const traditionalResult = await client.query<[{ result: any }]>(
        'MAP $value as result',
        { value: 'hello world' }
    );
    
    console.log('Traditional (Value object):', typeof traditionalResult[0][0].result); // 'object'
    console.log('Traditional value:', traditionalResult[0][0].result.value); // 'hello world'

    // Schema approach (returns primitives)
    const schemaResult = await client.queryWithSchema<{
        result: string;
    }>(
        'MAP $value as result',
        { value: 'hello world' },
        {
            params: Schema.object({ value: Schema.string() }),
            result: Schema.object({ result: Schema.string() })
        }
    );
    
    console.log('Schema (primitive):', typeof schemaResult[0][0].result); // 'string'
    console.log('Schema value:', schemaResult[0][0].result); // 'hello world'

    client.disconnect();
}

// Schema Builder Patterns
export class DatabaseSchemas {
    // Common user schema
    static readonly user = Schema.object({
        id: Schema.number(),
        name: Schema.string(),
        email: Schema.string(),
        createdAt: Schema.date(),
        isActive: Schema.boolean()
    });

    // Pagination schema
    static readonly pagination = Schema.object({
        limit: Schema.number(),
        offset: Schema.number()
    });

    // Search schema
    static readonly search = Schema.object({
        query: Schema.string(),
        filters: Schema.optional(Schema.object({
            category: Schema.optional(Schema.string()),
            dateRange: Schema.optional(Schema.object({
                start: Schema.date(),
                end: Schema.date()
            }))
        }))
    });
}

// Reusable query functions with schemas
export class UserQueries {
    static async findById(client: any, userId: number) {
        const schema: BidirectionalSchema = {
            params: Schema.object({ id: Schema.number() }),
            result: DatabaseSchemas.user
        };

        return client.queryWithSchema<{
            id: number;
            name: string;
            email: string;
            createdAt: Date;
            isActive: boolean;
        }>(
            'MAP { $id as id, "User Name" as name, "user@example.com" as email, NOW() as createdAt, TRUE as isActive }',
            { id: userId },
            schema
        );
    }

    static async search(client: any, searchQuery: string, limit: number = 10) {
        const schema: BidirectionalSchema = {
            params: Schema.object({
                query: Schema.string(),
                limit: Schema.number()
            }),
            result: Schema.array(DatabaseSchemas.user)
        };

        return client.queryWithSchema(
            'MAP { 1 as id, $query as name, "search@example.com" as email, NOW() as createdAt, TRUE as isActive }',
            { query: searchQuery, limit },
            schema
        );
    }
}

// Advanced schema patterns
export const AdvancedSchemas = {
    // API response wrapper
    apiResponse: <T>(dataSchema: any) => Schema.object({
        success: Schema.boolean(),
        data: dataSchema,
        error: Schema.optional(Schema.string()),
        timestamp: Schema.date()
    }),

    // Audit log schema
    auditLog: Schema.object({
        id: Schema.uuid7Value(), // Keep as Value object for UUIDs
        userId: Schema.number(),
        action: Schema.string(),
        resource: Schema.string(),
        timestamp: Schema.date(),
        metadata: Schema.optional(Schema.auto()) // JSON data
    })
};

// Type inference examples
type UserType = typeof DatabaseSchemas.user; // Schema type
// InferSchemaType<UserType> would give us the TypeScript type

export default schemaExamples;