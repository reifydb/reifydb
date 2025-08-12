/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { Schema, InferSchema, InferSchemas, SchemaNode } from './index';

class WsClient {
    async command<const S extends readonly SchemaNode[]>(
        statements: string | string[],
        params: any,
        schemas: S
    ): Promise<InferSchemas<S>> {
        const statementArray = Array.isArray(statements) ? statements : [statements];

        const frames = schemas.map((schema, index) => {
            const mockRow = this.mockDataForSchema(schema);
            return [mockRow];
        });

        return frames as InferSchemas<S>;
    }

    private mockDataForSchema(schema: SchemaNode): any {
        if (schema.kind === 'primitive') {
            switch (schema.type) {
                case 'Bool': return true;
                case 'Float4':
                case 'Float8': return 42.5;
                case 'Int1':
                case 'Int2':
                case 'Int4': return 42;
                case 'Int8':
                case 'Int16': return BigInt(999999999999);
                case 'Uint1':
                case 'Uint2':
                case 'Uint4': return 100;
                case 'Uint8':
                case 'Uint16': return BigInt(100);
                case 'Utf8': return 'test string';
                case 'Date':
                case 'DateTime': return new Date('2025-01-11');
                case 'Time': return '14:30:00';
                case 'Interval': return 'P1Y2M3D';
                case 'Uuid4': return '550e8400-e29b-41d4-a716-446655440000';
                case 'Uuid7': return '01934d3e-f7b0-7c13-a2d5-b85acd2f5e47';
                case 'RowId': return 'row_123456';
                case 'Blob': return new Uint8Array([1, 2, 3, 4]);
                case 'Undefined': return undefined;
                default: return null;
            }
        }

        if (schema.kind === 'object') {
            const result: any = {};
            for (const [key, propSchema] of Object.entries(schema.properties)) {
                result[key] = this.mockDataForSchema(propSchema);
            }
            return result;
        }

        if (schema.kind === 'array') {
            return [this.mockDataForSchema(schema.items)];
        }

        if (schema.kind === 'optional') {
            return Math.random() > 0.5 ? this.mockDataForSchema(schema.schema) : undefined;
        }

        return null;
    }
}

async function testPrimitiveTypes() {
    console.log('\n=== Testing Primitive Types ===');
    const wsClient = new WsClient();

    const frames = await wsClient.command(
        [
            'MAP $bool as result',
            'MAP $number as count',
            'MAP $string as name',
            'MAP $date as created',
            'MAP $uuid as id',
            'MAP $bigint as total'
        ],
        {
            bool: true,
            number: 42,
            string: 'test',
            date: new Date(),
            uuid: '550e8400-e29b-41d4-a716-446655440000',
            bigint: BigInt(999999999999)
        },
        [
            Schema.object({ result: Schema.bool() }),
            Schema.object({ count: Schema.int4() }),
            Schema.object({ name: Schema.string() }),
            Schema.object({ created: Schema.datetime() }),
            Schema.object({ id: Schema.uuid4() }),
            Schema.object({ total: Schema.int8() })
        ]
    );

    const boolFrame = frames[0];
    const numberFrame = frames[1];
    const stringFrame = frames[2];
    const dateFrame = frames[3];
    const uuidFrame = frames[4];
    const bigintFrame = frames[5];

    const boolResult: boolean = boolFrame[0].result;
    const numberResult: number = numberFrame[0].count;
    const stringResult: string = stringFrame[0].name;
    const dateResult: Date = dateFrame[0].created;
    const uuidResult: string = uuidFrame[0].id;
    const bigintResult: bigint = bigintFrame[0].total;

    console.log('Bool result:', boolResult, '- Type:', typeof boolResult);
    console.log('Number result:', numberResult, '- Type:', typeof numberResult);
    console.log('String result:', stringResult, '- Type:', typeof stringResult);
    console.log('Date result:', dateResult, '- Type:', dateResult instanceof Date ? 'Date' : typeof dateResult);
    console.log('UUID result:', uuidResult, '- Type:', typeof uuidResult);
    console.log('BigInt result:', bigintResult, '- Type:', typeof bigintResult);
}

async function testAllValueTypes() {
    console.log('\n=== Testing All Value Types ===');
    const wsClient = new WsClient();

    const frames = await wsClient.command(
        'MAP multiple values',
        {},
        [
            Schema.object({
                blob: Schema.blob(),
                bool: Schema.bool(),
                float4: Schema.float4(),
                float8: Schema.float8(),
                int1: Schema.int1(),
                int2: Schema.int2(),
                int4: Schema.int4(),
                int8: Schema.int8(),
                int16: Schema.int16(),
                uint1: Schema.uint1(),
                uint2: Schema.uint2(),
                uint4: Schema.uint4(),
                uint8: Schema.uint8(),
                uint16: Schema.uint16(),
                utf8: Schema.utf8(),
                date: Schema.date(),
                datetime: Schema.datetime(),
                time: Schema.time(),
                interval: Schema.interval(),
                uuid4: Schema.uuid4(),
                uuid7: Schema.uuid7(),
                rowid: Schema.rowid(),
                undef: Schema.undefined()
            })
        ]
    );

    const result = frames[0][0];

    console.log('Blob:', result.blob, '- Type:', result.blob instanceof Uint8Array ? 'Uint8Array' : typeof result.blob);
    console.log('Bool:', result.bool, '- Type:', typeof result.bool);
    console.log('Float4:', result.float4, '- Type:', typeof result.float4);
    console.log('Float8:', result.float8, '- Type:', typeof result.float8);
    console.log('Int1:', result.int1, '- Type:', typeof result.int1);
    console.log('Int8:', result.int8, '- Type:', typeof result.int8);
    console.log('Uint8:', result.uint8, '- Type:', typeof result.uint8);
    console.log('Utf8:', result.utf8, '- Type:', typeof result.utf8);
    console.log('Date:', result.date, '- Type:', result.date instanceof Date ? 'Date' : typeof result.date);
    console.log('Time:', result.time, '- Type:', typeof result.time);
    console.log('UUID4:', result.uuid4, '- Type:', typeof result.uuid4);
    console.log('RowId:', result.rowid, '- Type:', typeof result.rowid);
    console.log('Undefined:', result.undef, '- Type:', typeof result.undef);
}

async function testComplexTypes() {
    console.log('\n=== Testing Complex Types ===');
    const wsClient = new WsClient();

    const frames = await wsClient.command(
        [
            'MAP nested object',
            'MAP array of numbers',
            'MAP optional value',
            'MAP nullable value'
        ],
        {},
        [
            Schema.object({
                user: Schema.object({
                    id: Schema.int4(),
                    name: Schema.string(),
                    email: Schema.string(),
                    metadata: Schema.object({
                        created: Schema.datetime(),
                        active: Schema.bool()
                    })
                })
            }),
            Schema.object({
                numbers: Schema.array(Schema.int4())
            }),
            Schema.object({
                maybeValue: Schema.optional(Schema.string())
            }),
        ]
    );

    const nestedFrame = frames[0];
    const arrayFrame = frames[1];
    const optionalFrame = frames[2];

    const userId: number = nestedFrame[0].user.id;
    const userName: string = nestedFrame[0].user.name;
    const userCreated: Date = nestedFrame[0].user.metadata.created;
    const userActive: boolean = nestedFrame[0].user.metadata.active;

    const numbers: number[] = arrayFrame[0].numbers;
    const maybeValue: string | undefined = optionalFrame[0].maybeValue;

    console.log('Nested object - User ID:', userId, '- Type:', typeof userId);
    console.log('Nested object - User Name:', userName, '- Type:', typeof userName);
    console.log('Nested object - User Created:', userCreated, '- Type:', userCreated instanceof Date ? 'Date' : typeof userCreated);
    console.log('Nested object - User Active:', userActive, '- Type:', typeof userActive);
    console.log('Array of numbers:', numbers, '- Type:', Array.isArray(numbers) ? 'Array' : typeof numbers);
    console.log('Optional value:', maybeValue, '- Type:', typeof maybeValue);
}

async function testTypeInference() {
    console.log('\n=== Testing Type Inference ===');

    const userSchema = Schema.object({
        id: Schema.int4(),
        name: Schema.string(),
        email: Schema.string(),
        isActive: Schema.bool(),
        createdAt: Schema.datetime(),
        profile: Schema.object({
            bio: Schema.optional(Schema.string()),
            tags: Schema.array(Schema.string())
        })
    });

    type User = InferSchema<typeof userSchema>;

    const testUser: User = {
        id: 123,
        name: "John Doe",
        email: "john@example.com",
        isActive: true,
        createdAt: new Date(),
        profile: {
            bio: "Developer",
            tags: ["typescript", "nodejs"]
        }
    };

    console.log('Type inference test passed! User object:', testUser);
    console.log('User ID type:', typeof testUser.id);
    console.log('User name type:', typeof testUser.name);
    console.log('User isActive type:', typeof testUser.isActive);
    console.log('User createdAt type:', testUser.createdAt instanceof Date ? 'Date' : typeof testUser.createdAt);
    console.log('User profile.bio type:', typeof testUser.profile.bio);
    console.log('User profile.tags type:', Array.isArray(testUser.profile.tags) ? 'Array' : typeof testUser.profile.tags);
}

async function runAllTests() {
    console.log('Starting Schema Implementation Tests...');

    try {
        await testPrimitiveTypes();
        await testAllValueTypes();
        await testComplexTypes();
        await testTypeInference();

        console.log('\n✅ All tests passed! Schema implementation with proper type inference is working correctly.');
    } catch (error) {
        console.error('\n❌ Test failed:', error);
    }
}

runAllTests().catch(console.error);