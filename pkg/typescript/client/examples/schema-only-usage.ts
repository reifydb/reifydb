/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

/**
 * Schema-Only ReifyDB Client Usage Examples
 * 
 * This demonstrates the new schema-only approach where all operations
 * require explicit schemas for type safety and automatic conversion.
 */

import {
    Client,
    Schema,
    BidirectionalSchema,
    SchemaHelpers,
    DEFAULT_SCHEMA
} from '@reifydb/client';

async function schemaOnlyExamples() {
    // Connect to ReifyDB
    const client = await Client.connect_ws('ws://localhost:8090/ws');

    // Example 1: Simple query with primitive results
    const userSchema: BidirectionalSchema = {
        params: Schema.object({
            userId: Schema.number()
        }),
        result: Schema.object({
            id: Schema.number(),        // Returns number, not Int4Value
            name: Schema.string(),      // Returns string, not Utf8Value
            isActive: Schema.boolean()  // Returns boolean, not BoolValue
        })
    };

    const users = await client.query<{
        id: number;
        name: string;
        isActive: boolean;
    }>(
        'MAP { $userId as id, "John Doe" as name, TRUE as isActive }',
        { userId: 123 },
        userSchema
    );

    console.log('User:', users[0][0]);
    // users[0][0].id is number, not Value object!
    // users[0][0].name is string, not Value object!

    // Example 2: Using helper schemas for simple cases
    const simpleResult = await client.command(
        'MAP "Hello World" as message',
        DEFAULT_SCHEMA // Accepts any params, returns Value objects
    );

    console.log('Simple result:', simpleResult[0][0].message.value);

    // Example 3: Schema-only approach without parameters
    const constantSchema: BidirectionalSchema = {
        result: Schema.object({
            timestamp: Schema.date(),
            count: Schema.number()
        })
    };

    const constants = await client.query<{
        timestamp: Date;
        count: number;
    }>(
        'MAP { NOW() as timestamp, 42 as count }',
        constantSchema  // No params needed
    );

    console.log('Constants:', constants[0][0]);

    // Example 4: Mixed primitive and Value object handling
    const mixedSchema: BidirectionalSchema = {
        params: Schema.object({
            text: Schema.string(),
            number: Schema.number(),
            flag: Schema.boolean()
        }),
        result: Schema.object({
            // Return some as primitives
            processed_text: Schema.string(),
            calculated_value: Schema.number(),
            // Keep some as Value objects for advanced handling
            raw_flag: Schema.boolValue(),
            timestamp: Schema.dateTimeValue()
        })
    };

    const mixed = await client.command<{
        processed_text: string;
        calculated_value: number;
        raw_flag: any; // BoolValue
        timestamp: any; // DateTimeValue
    }>(
        'MAP { UPPER($text) as processed_text, ($number * 2) as calculated_value, $flag as raw_flag, NOW() as timestamp }',
        {
            text: "hello",
            number: 21,
            flag: true
        },
        mixedSchema
    );

    const result = mixed[0][0];
    console.log('Processed text:', result.processed_text); // string
    console.log('Calculated value:', result.calculated_value); // number
    console.log('Raw flag:', result.raw_flag.value); // BoolValue.value
    console.log('Timestamp:', result.timestamp.value); // DateTimeValue.value

    // Example 5: Array parameters with schema
    const arraySchema: BidirectionalSchema = {
        params: Schema.array(Schema.string()),
        result: Schema.object({
            first: Schema.string(),
            second: Schema.string(),
            third: Schema.string()
        })
    };

    const arrayResult = await client.query<{
        first: string;
        second: string;
        third: string;
    }>(
        'MAP { $1 as first, $2 as second, $3 as third }',
        ['apple', 'banana', 'cherry'],
        arraySchema
    );

    console.log('Array result:', arrayResult[0][0]);

    // Example 6: Optional parameters and complex nested data
    const complexSchema: BidirectionalSchema = {
        params: Schema.object({
            user: Schema.object({
                name: Schema.string(),
                age: Schema.number(),
                preferences: Schema.optional(Schema.object({
                    theme: Schema.string(),
                    notifications: Schema.boolean()
                }))
            })
        }),
        result: Schema.object({
            greeting: Schema.string(),
            summary: Schema.string()
        })
    };

    const complexResult = await client.command<{
        greeting: string;
        summary: string;
    }>(
        'MAP { CONCAT("Hello ", $user.name) as greeting, CONCAT("Age: ", CAST($user.age as TEXT)) as summary }',
        {
            user: {
                name: "Alice",
                age: 30,
                preferences: {
                    theme: "dark",
                    notifications: true
                }
            }
        },
        complexSchema
    );

    console.log('Complex result:', complexResult[0][0]);

    // Example 7: Using SchemaHelpers for common patterns
    
    // Flexible schema for testing/development
    const flexibleResult = await client.query(
        'MAP { 1 as id, "test" as name }',
        SchemaHelpers.flexibleSchema()
    );
    
    // Auto-detect everything
    const autoResult = await client.command(
        'MAP $value as result',
        { value: "auto-detected" },
        {
            params: Schema.auto(),
            result: Schema.object({ result: Schema.string() })
        }
    );

    console.log('Auto result:', autoResult[0][0].result); // string

    client.disconnect();
}

// Reusable Schema Patterns
export class DatabaseSchemas {
    // User entity with primitives
    static readonly USER_PRIMITIVE = Schema.object({
        id: Schema.number(),
        email: Schema.string(),
        name: Schema.string(),
        createdAt: Schema.date(),
        isActive: Schema.boolean()
    });

    // User entity with Value objects (for advanced processing)
    static readonly USER_VALUES = Schema.object({
        id: Schema.int4Value(),
        email: Schema.utf8Value(),
        name: Schema.utf8Value(),
        createdAt: Schema.dateTimeValue(),
        isActive: Schema.boolValue()
    });

    // Pagination parameters
    static readonly PAGINATION_PARAMS = Schema.object({
        limit: Schema.number(),
        offset: Schema.number(),
        sortBy: Schema.optional(Schema.string()),
        sortOrder: Schema.optional(Schema.union(
            Schema.string(), // "asc" | "desc"
        ))
    });

    // API response wrapper
    static apiResponse<T>(dataSchema: T) {
        return Schema.object({
            success: Schema.boolean(),
            data: dataSchema,
            error: Schema.optional(Schema.string()),
            timestamp: Schema.date()
        });
    }
}

// Typed query functions using schemas
export class UserRepository {
    constructor(private client: any) {}

    async findById(id: number) {
        const schema: BidirectionalSchema = {
            params: Schema.object({ id: Schema.number() }),
            result: DatabaseSchemas.USER_PRIMITIVE
        };

        return this.client.query<{
            id: number;
            email: string;
            name: string;
            createdAt: Date;
            isActive: boolean;
        }>(
            'MAP { $id as id, "user@example.com" as email, "User Name" as name, NOW() as createdAt, TRUE as isActive }',
            { id },
            schema
        );
    }

    async create(userData: { email: string; name: string }) {
        const schema: BidirectionalSchema = {
            params: Schema.object({
                email: Schema.string(),
                name: Schema.string()
            }),
            result: DatabaseSchemas.USER_PRIMITIVE
        };

        return this.client.command<{
            id: number;
            email: string;
            name: string;
            createdAt: Date;
            isActive: boolean;
        }>(
            'MAP { 1 as id, $email as email, $name as name, NOW() as createdAt, TRUE as isActive }',
            userData,
            schema
        );
    }

    async search(query: string, pagination: { limit: number; offset: number }) {
        const schema: BidirectionalSchema = {
            params: Schema.object({
                query: Schema.string(),
                limit: Schema.number(),
                offset: Schema.number()
            }),
            result: Schema.array(DatabaseSchemas.USER_PRIMITIVE)
        };

        return this.client.query(
            'MAP { 1 as id, $query as email, "Search Result" as name, NOW() as createdAt, TRUE as isActive }',
            { query, ...pagination },
            schema
        );
    }
}

// Key Benefits of Schema-Only Approach:
// 1. **Type Safety**: Full TypeScript inference from schema to results
// 2. **Automatic Conversion**: Primitives ↔ Value objects handled automatically  
// 3. **Explicit Intent**: Every operation declares its expected types
// 4. **Consistent API**: All methods follow the same pattern
// 5. **Flexibility**: Mix primitives and Value objects as needed
// 6. **Validation**: Runtime type checking via schemas
// 7. **Documentation**: Schemas serve as living documentation

export default schemaOnlyExamples;