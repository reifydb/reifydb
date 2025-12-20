/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {afterAll, beforeAll, describe, expect, it} from 'vitest';
import {renderHook, waitFor} from '@testing-library/react';
import {useSchema, getConnection, clearConnection, Client, ConnectionProvider} from '../../../src';
import {waitForDatabase} from '../setup';
// @ts-ignore
import React from 'react';

const TEST_NAMESPACE = `test_schema_${Date.now()}`;

describe('useSchema Hook', () => {
    let setupClient: Awaited<ReturnType<typeof Client.connect_ws>> | null = null;

    const wrapper = ({children}: {children: React.ReactNode}) => (
        <ConnectionProvider config={{url: 'ws://127.0.0.1:8090'}} children={children} />
    );

    beforeAll(async () => {
        await waitForDatabase();

        // Create test namespace and tables
        const url = process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:8090';
        setupClient = await Client.connect_ws(url, {timeoutMs: 10000});

        // Create namespace
        await setupClient.command(`CREATE NAMESPACE ${TEST_NAMESPACE}`, {}, []);

        // Create tables with all supported column types
        // Table 1: Integer types
        await setupClient.command(
            `CREATE TABLE ${TEST_NAMESPACE}.types_integers {
                col_int1: INT1,
                col_int2: INT2,
                col_int4: INT4,
                col_int8: INT8,
                col_int16: INT16,
                col_int: INT,
                col_uint1: UINT1,
                col_uint2: UINT2,
                col_uint4: UINT4,
                col_uint8: UINT8,
                col_uint16: UINT16,
                col_uint: UINT
            }`,
            {},
            []
        );

        // Table 2: Float types
        await setupClient.command(
            `CREATE TABLE ${TEST_NAMESPACE}.types_floats {
                col_float4: FLOAT4,
                col_float8: FLOAT8,
                col_decimal: DECIMAL
            }`,
            {},
            []
        );

        // Table 3: Text and binary types
        await setupClient.command(
            `CREATE TABLE ${TEST_NAMESPACE}.types_text {
                col_utf8: UTF8,
                col_blob: BLOB
            }`,
            {},
            []
        );

        // Table 4: Temporal types
        await setupClient.command(
            `CREATE TABLE ${TEST_NAMESPACE}.types_temporal {
                col_date: DATE,
                col_datetime: DATETIME,
                col_time: TIME,
                col_duration: DURATION
            }`,
            {},
            []
        );

        // Table 5: Identifier types
        await setupClient.command(
            `CREATE TABLE ${TEST_NAMESPACE}.types_identifiers {
                col_uuid4: UUID4,
                col_uuid7: UUID7
            }`,
            {},
            []
        );

        // Table 6: Misc types
        await setupClient.command(
            `CREATE TABLE ${TEST_NAMESPACE}.types_misc {
                col_boolean: BOOLEAN
            }`,
            {},
            []
        );
    }, 60000);

    afterAll(async () => {
        // Drop test namespace and tables
        if (setupClient) {
            try {
                await setupClient.command(`DROP NAMESPACE ${TEST_NAMESPACE} CASCADE`, {}, []);
            } catch (e) {
                // Ignore cleanup errors
            }
            setupClient.disconnect();
        }
        await clearConnection();
    });

    it('should return loading state initially', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        // Initially should be loading
        expect(result.current[0]).toBe(true);
    });

    it('should fetch schema and return table info', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false); // isLoading
            },
            {timeout: 10000}
        );

        const [isLoading, schema, error] = result.current;

        expect(isLoading).toBe(false);
        expect(error).toBeUndefined();
        expect(schema.length).toBeGreaterThan(0);

        // Find our test tables
        const testTables = schema.filter((t) => t.name.startsWith(`${TEST_NAMESPACE}.`));
        expect(testTables.length).toBe(6);

        // Verify table names
        const tableNames = testTables.map((t) => t.name).sort();
        expect(tableNames).toContain(`${TEST_NAMESPACE}.types_integers`);
        expect(tableNames).toContain(`${TEST_NAMESPACE}.types_floats`);
        expect(tableNames).toContain(`${TEST_NAMESPACE}.types_text`);
        expect(tableNames).toContain(`${TEST_NAMESPACE}.types_temporal`);
        expect(tableNames).toContain(`${TEST_NAMESPACE}.types_identifiers`);
        expect(tableNames).toContain(`${TEST_NAMESPACE}.types_misc`);
    });

    it('should correctly map integer column types', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false);
            },
            {timeout: 10000}
        );

        const [, schema] = result.current;
        const integersTable = schema.find((t) => t.name === `${TEST_NAMESPACE}.types_integers`);

        expect(integersTable).toBeDefined();
        expect(integersTable!.columns).toHaveLength(12);

        const columnTypeMap = new Map(integersTable!.columns.map((c) => [c.name, c.dataType]));

        expect(columnTypeMap.get('col_int1')).toBe('Int1');
        expect(columnTypeMap.get('col_int2')).toBe('Int2');
        expect(columnTypeMap.get('col_int4')).toBe('Int4');
        expect(columnTypeMap.get('col_int8')).toBe('Int8');
        expect(columnTypeMap.get('col_int16')).toBe('Int16');
        expect(columnTypeMap.get('col_int')).toBe('Int');
        expect(columnTypeMap.get('col_uint1')).toBe('Uint1');
        expect(columnTypeMap.get('col_uint2')).toBe('Uint2');
        expect(columnTypeMap.get('col_uint4')).toBe('Uint4');
        expect(columnTypeMap.get('col_uint8')).toBe('Uint8');
        expect(columnTypeMap.get('col_uint16')).toBe('Uint16');
        expect(columnTypeMap.get('col_uint')).toBe('Uint');
    });

    it('should correctly map float column types', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false);
            },
            {timeout: 10000}
        );

        const [, schema] = result.current;
        const floatsTable = schema.find((t) => t.name === `${TEST_NAMESPACE}.types_floats`);

        expect(floatsTable).toBeDefined();
        expect(floatsTable!.columns).toHaveLength(3);

        const columnTypeMap = new Map(floatsTable!.columns.map((c) => [c.name, c.dataType]));

        expect(columnTypeMap.get('col_float4')).toBe('Float4');
        expect(columnTypeMap.get('col_float8')).toBe('Float8');
        expect(columnTypeMap.get('col_decimal')).toBe('Decimal');
    });

    it('should correctly map text and binary column types', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false);
            },
            {timeout: 10000}
        );

        const [, schema] = result.current;
        const textTable = schema.find((t) => t.name === `${TEST_NAMESPACE}.types_text`);

        expect(textTable).toBeDefined();
        expect(textTable!.columns).toHaveLength(2);

        const columnTypeMap = new Map(textTable!.columns.map((c) => [c.name, c.dataType]));

        expect(columnTypeMap.get('col_utf8')).toBe('Utf8');
        expect(columnTypeMap.get('col_blob')).toBe('Blob');
    });

    it('should correctly map temporal column types', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false);
            },
            {timeout: 10000}
        );

        const [, schema] = result.current;
        const temporalTable = schema.find((t) => t.name === `${TEST_NAMESPACE}.types_temporal`);

        expect(temporalTable).toBeDefined();
        expect(temporalTable!.columns).toHaveLength(4);

        const columnTypeMap = new Map(temporalTable!.columns.map((c) => [c.name, c.dataType]));

        expect(columnTypeMap.get('col_date')).toBe('Date');
        expect(columnTypeMap.get('col_datetime')).toBe('DateTime');
        expect(columnTypeMap.get('col_time')).toBe('Time');
        expect(columnTypeMap.get('col_duration')).toBe('Duration');
    });

    it('should correctly map identifier column types', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false);
            },
            {timeout: 10000}
        );

        const [, schema] = result.current;
        const identifiersTable = schema.find((t) => t.name === `${TEST_NAMESPACE}.types_identifiers`);

        expect(identifiersTable).toBeDefined();
        expect(identifiersTable!.columns).toHaveLength(2);

        const columnTypeMap = new Map(identifiersTable!.columns.map((c) => [c.name, c.dataType]));

        expect(columnTypeMap.get('col_uuid4')).toBe('Uuid4');
        expect(columnTypeMap.get('col_uuid7')).toBe('Uuid7');
        // col_identity_id: IDENTITY_ID - not yet supported
    });

    it('should correctly map boolean column type', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false);
            },
            {timeout: 10000}
        );

        const [, schema] = result.current;
        const miscTable = schema.find((t) => t.name === `${TEST_NAMESPACE}.types_misc`);

        expect(miscTable).toBeDefined();
        expect(miscTable!.columns).toHaveLength(1);
        expect(miscTable!.columns[0].name).toBe('col_boolean');
        expect(miscTable!.columns[0].dataType).toBe('Boolean');
    });

    it('should preserve column order by position', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false);
            },
            {timeout: 10000}
        );

        const [, schema] = result.current;
        const integersTable = schema.find((t) => t.name === `${TEST_NAMESPACE}.types_integers`);

        expect(integersTable).toBeDefined();

        // Columns should be in the order they were defined
        const columnNames = integersTable!.columns.map((c) => c.name);
        expect(columnNames[0]).toBe('col_int1');
        expect(columnNames[1]).toBe('col_int2');
        expect(columnNames[2]).toBe('col_int4');
        expect(columnNames[3]).toBe('col_int8');
        expect(columnNames[4]).toBe('col_int16');
        expect(columnNames[5]).toBe('col_int');
        expect(columnNames[6]).toBe('col_uint1');
        expect(columnNames[7]).toBe('col_uint2');
        expect(columnNames[8]).toBe('col_uint4');
        expect(columnNames[9]).toBe('col_uint8');
        expect(columnNames[10]).toBe('col_uint16');
        expect(columnNames[11]).toBe('col_uint');
    });

    it('should return tables sorted alphabetically by name', async () => {
        const {result} = renderHook(() => useSchema(), {wrapper});

        await waitFor(
            () => {
                expect(result.current[0]).toBe(false);
            },
            {timeout: 10000}
        );

        const [, schema] = result.current;

        // Verify schema is sorted
        const tableNames = schema.map((t) => t.name);
        const sortedNames = [...tableNames].sort((a, b) => a.localeCompare(b));
        expect(tableNames).toEqual(sortedNames);
    });
});
