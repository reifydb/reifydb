/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { expect } from 'vitest';

/**
 * Validates a single-frame, single-row result with expected value and type
 * @param frames - The result frames from a query/command
 * @param expectedValue - The expected value of the result field
 * @param expectedType - The expected JavaScript type of the result field
 * @param fieldName - The name of the field to check (defaults to 'result')
 */
export function expectSingleResult<T>(
    frames: readonly any[] | any[][],
    expectedValue: T,
    expectedType: string,
    fieldName: string = 'result'
): void {
    expect(frames).toHaveLength(1);
    expect(frames[0]).toHaveLength(1);
    expect(frames[0][0][fieldName]).toBe(expectedValue);
    expect(typeof frames[0][0][fieldName]).toBe(expectedType);
}

/**
 * Validates a single-frame, single-row result for BigInt values
 * @param frames - The result frames from a query/command
 * @param expectedValue - The expected BigInt value
 * @param fieldName - The name of the field to check (defaults to 'result')
 */
export function expectSingleBigIntResult(
    frames: readonly any[] | any[][],
    expectedValue: bigint,
    fieldName: string = 'result'
): void {
    expect(frames).toHaveLength(1);
    expect(frames[0]).toHaveLength(1);
    expect(frames[0][0][fieldName]).toBe(expectedValue);
    expect(typeof frames[0][0][fieldName]).toBe('bigint');
}

/**
 * Validates a single-frame, single-row result for Date values
 * @param frames - The result frames from a query/command
 * @param expectedValue - The expected Date value or ISO string
 * @param fieldName - The name of the field to check (defaults to 'result')
 */
export function expectSingleDateResult(
    frames: readonly any[] | any[][],
    expectedValue: Date | string,
    fieldName: string = 'result'
): void {
    expect(frames).toHaveLength(1);
    expect(frames[0]).toHaveLength(1);
    
    const result = frames[0][0][fieldName];
    expect(result).toBeInstanceOf(Date);
    
    if (typeof expectedValue === 'string') {
        expect(result.toISOString()).toBe(expectedValue);
    } else {
        expect(result.getTime()).toBe(expectedValue.getTime());
    }
}

/**
 * Validates a single-frame, single-row result for Uint8Array values
 * @param frames - The result frames from a query/command
 * @param expectedValue - The expected Uint8Array value
 * @param fieldName - The name of the field to check (defaults to 'result')
 */
export function expectSingleBlobResult(
    frames: readonly any[] | any[][],
    expectedValue: Uint8Array,
    fieldName: string = 'result'
): void {
    expect(frames).toHaveLength(1);
    expect(frames[0]).toHaveLength(1);
    
    const result = frames[0][0][fieldName];
    expect(result).toBeInstanceOf(Uint8Array);
    expect(result).toEqual(expectedValue);
}
