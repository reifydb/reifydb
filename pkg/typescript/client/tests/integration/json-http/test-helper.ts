// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import { expect } from 'vitest';

/**
 * Validates a single-frame, single-row JSON result with expected value and type
 */
export function expectSingleResult<T>(
    frames: any,
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
 * Validates a single-frame, single-row JSON result for string date values
 * (JSON format returns dates as ISO strings, not Date objects)
 */
export function expectSingleDateResult(
    frames: any,
    expectedValue: string,
    fieldName: string = 'result'
): void {
    expect(frames).toHaveLength(1);
    expect(frames[0]).toHaveLength(1);

    const result = frames[0][0][fieldName];
    expect(typeof result).toBe('string');
    expect(result).toBe(expectedValue);
}

/**
 * Validates a single-frame, single-row JSON result for null values
 */
export function expectSingleNullResult(
    frames: any,
    fieldName: string = 'result'
): void {
    expect(frames).toHaveLength(1);
    expect(frames[0]).toHaveLength(1);
    expect(frames[0][0][fieldName]).toBeNull();
}
