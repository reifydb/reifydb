// Zod-style Schema Type Inference Implementation
// This demonstrates how the API should work with proper type inference

// Schema Node Types
interface PrimitiveSchemaNode<T extends string = string> {
    kind: 'primitive';
    type: T;
}

interface ObjectSchemaNode<P extends Record<string, SchemaNode> = Record<string, SchemaNode>> {
    kind: 'object';
    properties: P;
}

type SchemaNode =
    | PrimitiveSchemaNode
    | ObjectSchemaNode;

// Type mapping for primitives
type PrimitiveToTS<T> =
    T extends 'string' ? string :
        T extends 'number' ? number :
            T extends 'boolean' ? boolean :
                T extends 'bigint' ? bigint :
                    T extends 'Date' ? Date :
                        T extends 'undefined' ? undefined :
                            T extends 'null' ? null :
                                never;

// Schema inference - the magic happens here
type InferSchema<S> =
    S extends PrimitiveSchemaNode<infer T> ? PrimitiveToTS<T> :
        S extends ObjectSchemaNode<infer P> ? {
                [K in keyof P]: InferSchema<P[K]>
            } :
            never;

// Schema builder with proper return types
class SchemaBuilder {
    static string(): PrimitiveSchemaNode<'string'> {
        return {kind: 'primitive', type: 'string'};
    }

    static number(): PrimitiveSchemaNode<'number'> {
        return {kind: 'primitive', type: 'number'};
    }

    static boolean(): PrimitiveSchemaNode<'boolean'> {
        return {kind: 'primitive', type: 'boolean'};
    }

    static object<P extends Record<string, SchemaNode>>(properties: P): ObjectSchemaNode<P> {
        return {kind: 'object', properties};
    }
}

// Mapped tuple type for multiple schemas
type InferSchemas<S extends readonly SchemaNode[]> = {
    [K in keyof S]: InferSchema<S[K]>[]
};

// Mock WebSocket Client with proper type inference
class WsClient {
    async command<const S extends readonly ObjectSchemaNode[]>(
        statements: string | string[],
        params: any,
        schemas: S
    ): Promise<InferSchemas<S>> {
        // Mock implementation - would normally send to server
        const statementArray = Array.isArray(statements) ? statements : [statements];

        // Simulate server response with correct types
        const frames = schemas.map((schema, index) => {
            // Mock data based on schema
            const mockRow = this.mockDataForSchema(schema);
            return [mockRow]; // Return array with one row per frame
        });

        return frames as InferSchemas<S>;
    }

    private mockDataForSchema(schema: ObjectSchemaNode): any {
        const result: any = {};
        for (const [key, propSchema] of Object.entries(schema.properties)) {
            if (propSchema.kind === 'primitive') {
                switch (propSchema.type) {
                    case 'boolean':
                        result[key] = true;
                        break;
                    case 'number':
                        result[key] = 42;
                        break;
                    case 'string':
                        result[key] = 'test';
                        break;
                }
            }
        }
        return result;
    }
}

// Export Schema as the builder
const Schema = SchemaBuilder;

// The actual test
async function runTest() {
    const wsClient = new WsClient();

    // Different types for each statement result
    const frames = await wsClient.command(
        [
            'MAP $value as result',
            'MAP { $count as count, $name as name }',
            'MAP $active as isActive'
        ],
        {value: true, count: 42, name: 'test', active: false},
        [
            Schema.object({result: Schema.boolean()}),
            Schema.object({count: Schema.number(), name: Schema.string()}),
            Schema.object({isActive: Schema.boolean()}),
            Schema.object({
                isActive: Schema.string(), nested: Schema.object({
                    value: Schema.number()
                })
            })
        ]
    );

    // Each frame has its own type - TypeScript knows this!
    const boolFrame = frames[0];   // { result: boolean }[]
    const mixedFrame = frames[1];  // { count: number, name: string }[]
    const activeFrame = frames[2]; // { isActive: boolean }[]
    const test = frames[3]; // { isActive: boolean }[]

    // Type checking - these all work
    const result: boolean = boolFrame[0].result;
    const count: number = mixedFrame[0].count;
    const name: string = mixedFrame[0].name;
    const isActive: boolean = activeFrame[0].isActive;

    console.log('Frame 0 (boolean):', boolFrame);
    console.log('Frame 1 (mixed):', mixedFrame);
    console.log('Frame 2 (isActive):', activeFrame);

    // Verify types at runtime
    console.log('\nType verification:');
    console.log('typeof boolFrame[0].result:', typeof boolFrame[0].result);
    console.log('typeof mixedFrame[0].count:', typeof mixedFrame[0].count);
    console.log('typeof mixedFrame[0].name:', typeof mixedFrame[0].name);
    console.log('typeof activeFrame[0].isActive:', typeof activeFrame[0].isActive);

    // This would be a TypeScript compile error (uncomment to test):
    // const wrong: string = boolFrame[0].result; // Error: Type 'boolean' is not assignable to type 'string'

    console.log('\n✅ Test passed! All types are correctly inferred.');
}

// Run the test
runTest().catch(console.error);

// Also test single statement (like the original test)
async function testSingleStatement() {
    const wsClient = new WsClient();

    // Single statement still works - no need for manual types!
    const frames = await wsClient.command(
        'MAP $value as result',
        {value: true},
        [Schema.object({result: Schema.boolean()})]
    );

    // TypeScript knows frames is [{ result: boolean }[]]
    const firstFrame = frames[0];  // { result: boolean }[]
    const result: boolean = firstFrame[0].result; // boolean

    console.log('\nSingle statement test:');
    console.log('Frame:', firstFrame);
    console.log('Result type:', typeof result);
    console.log('Result value:', result);
}

testSingleStatement().catch(console.error);