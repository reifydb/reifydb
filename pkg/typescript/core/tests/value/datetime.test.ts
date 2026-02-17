// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { describe, expect, it } from 'vitest';
import { DateTimeValue } from '../../src';

describe('DateTimeValue', () => {
    describe('constructor', () => {
        it('should create instance with Date object', () => {
            const jsDate = new Date('2024-03-15T14:30:45.123Z');
            const datetime = new DateTimeValue(jsDate);
            expect(datetime.value).toBeDefined();
            expect(datetime.type).toBe('DateTime');
        });

        it('should create instance with datetime string', () => {
            const datetime = new DateTimeValue('2024-03-15T14:30:45.123456789Z');
            expect(datetime.toString()).toBe('2024-03-15T14:30:45.123456789Z');
        });

        it('should create instance with milliseconds', () => {
            const datetime = new DateTimeValue(0);
            expect(datetime.toString()).toBe('1970-01-01T00:00:00.000000000Z');
        });

        it('should create instance with nanoseconds as bigint', () => {
            const datetime = new DateTimeValue(BigInt(123456789123456789));
            expect(datetime.value).toBeDefined();
        });

        it('should create instance with undefined', () => {
            const datetime = new DateTimeValue(undefined);
            expect(datetime.value).toBeUndefined();
            expect(datetime.toString()).toBe('none');
        });

        it('should throw error for invalid datetime string', () => {
            expect(() => new DateTimeValue('invalid')).toThrow('Invalid datetime string: invalid');
            expect(() => new DateTimeValue('2024-13-01T00:00:00Z')).toThrow('Invalid datetime string');
        });
    });

    describe('fromYMDHMSN', () => {
        it('should create datetime from components', () => {
            const datetime = DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123456789);
            expect(datetime.toString()).toBe('2024-03-15T14:30:45.123456789Z');
        });

        it('should default nano to 0', () => {
            const datetime = DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45);
            expect(datetime.toString()).toBe('2024-03-15T14:30:45.000000000Z');
        });

        it('should handle leap year', () => {
            const datetime = DateTimeValue.fromYMDHMSN(2024, 2, 29, 12, 0, 0, 0);
            expect(datetime.toString()).toBe('2024-02-29T12:00:00.000000000Z');
        });

        it('should throw error for invalid dates', () => {
            expect(() => DateTimeValue.fromYMDHMSN(2024, 2, 30, 0, 0, 0)).toThrow('Invalid datetime');
            expect(() => DateTimeValue.fromYMDHMSN(2024, 13, 1, 0, 0, 0)).toThrow('Invalid datetime');
        });

        it('should throw error for invalid times', () => {
            expect(() => DateTimeValue.fromYMDHMSN(2024, 3, 15, 24, 0, 0)).toThrow('Invalid hour: 24');
            expect(() => DateTimeValue.fromYMDHMSN(2024, 3, 15, 0, 60, 0)).toThrow('Invalid minute: 60');
            expect(() => DateTimeValue.fromYMDHMSN(2024, 3, 15, 0, 0, 60)).toThrow('Invalid second: 60');
            expect(() => DateTimeValue.fromYMDHMSN(2024, 3, 15, 0, 0, 0, 1_000_000_000)).toThrow('Invalid nanosecond');
        });
    });

    describe('display standard format', () => {
        it('should format standard datetimes correctly', () => {
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123456789).toString())
                .toBe('2024-03-15T14:30:45.123456789Z');
            expect(DateTimeValue.fromYMDHMSN(2000, 1, 1, 0, 0, 0, 0).toString())
                .toBe('2000-01-01T00:00:00.000000000Z');
            expect(DateTimeValue.fromYMDHMSN(1999, 12, 31, 23, 59, 59, 999999999).toString())
                .toBe('1999-12-31T23:59:59.999999999Z');
        });
    });

    describe('display millisecond precision', () => {
        it('should format millisecond values correctly', () => {
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123000000).toString())
                .toBe('2024-03-15T14:30:45.123000000Z');
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 1000000).toString())
                .toBe('2024-03-15T14:30:45.001000000Z');
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 999000000).toString())
                .toBe('2024-03-15T14:30:45.999000000Z');
        });
    });

    describe('display microsecond precision', () => {
        it('should format microsecond values correctly', () => {
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123456000).toString())
                .toBe('2024-03-15T14:30:45.123456000Z');
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 1000).toString())
                .toBe('2024-03-15T14:30:45.000001000Z');
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 999999000).toString())
                .toBe('2024-03-15T14:30:45.999999000Z');
        });
    });

    describe('display nanosecond precision', () => {
        it('should format nanosecond values correctly', () => {
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123456789).toString())
                .toBe('2024-03-15T14:30:45.123456789Z');
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 1).toString())
                .toBe('2024-03-15T14:30:45.000000001Z');
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 999999999).toString())
                .toBe('2024-03-15T14:30:45.999999999Z');
        });
    });

    describe('display zero fractional seconds', () => {
        it('should format zero fractional seconds', () => {
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 0).toString())
                .toBe('2024-03-15T14:30:45.000000000Z');
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 0, 0, 0, 0).toString())
                .toBe('2024-03-15T00:00:00.000000000Z');
        });
    });

    describe('display edge times', () => {
        it('should format edge times correctly', () => {
            // Midnight
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 0, 0, 0, 0).toString())
                .toBe('2024-03-15T00:00:00.000000000Z');
            // Almost midnight next day
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 23, 59, 59, 999999999).toString())
                .toBe('2024-03-15T23:59:59.999999999Z');
            // Noon
            expect(DateTimeValue.fromYMDHMSN(2024, 3, 15, 12, 0, 0, 0).toString())
                .toBe('2024-03-15T12:00:00.000000000Z');
        });
    });

    describe('display unix epoch', () => {
        it('should format unix epoch correctly', () => {
            expect(DateTimeValue.fromYMDHMSN(1970, 1, 1, 0, 0, 0, 0).toString())
                .toBe('1970-01-01T00:00:00.000000000Z');
            expect(DateTimeValue.fromYMDHMSN(1970, 1, 1, 0, 0, 1, 0).toString())
                .toBe('1970-01-01T00:00:01.000000000Z');
        });
    });

    describe('display leap year', () => {
        it('should format leap year dates correctly', () => {
            expect(DateTimeValue.fromYMDHMSN(2024, 2, 29, 12, 30, 45, 123456789).toString())
                .toBe('2024-02-29T12:30:45.123456789Z');
            expect(DateTimeValue.fromYMDHMSN(2000, 2, 29, 0, 0, 0, 0).toString())
                .toBe('2000-02-29T00:00:00.000000000Z');
        });
    });

    describe('display boundary dates', () => {
        it('should format boundary dates correctly', () => {
            // Very early date
            expect(DateTimeValue.fromYMDHMSN(1, 1, 1, 0, 0, 0, 0).toString())
                .toBe('0001-01-01T00:00:00.000000000Z');
            // Far future date
            expect(DateTimeValue.fromYMDHMSN(9999, 12, 31, 23, 59, 59, 999999999).toString())
                .toBe('9999-12-31T23:59:59.999999999Z');
            // Century boundaries
            expect(DateTimeValue.fromYMDHMSN(1900, 1, 1, 0, 0, 0, 0).toString())
                .toBe('1900-01-01T00:00:00.000000000Z');
            expect(DateTimeValue.fromYMDHMSN(2000, 1, 1, 0, 0, 0, 0).toString())
                .toBe('2000-01-01T00:00:00.000000000Z');
            expect(DateTimeValue.fromYMDHMSN(2100, 1, 1, 0, 0, 0, 0).toString())
                .toBe('2100-01-01T00:00:00.000000000Z');
        });
    });

    describe('display default', () => {
        it('should format default datetime correctly', () => {
            expect(DateTimeValue.default().toString()).toBe('1970-01-01T00:00:00.000000000Z');
        });
    });

    describe('display all hours', () => {
        it('should format all hours correctly', () => {
            for (let hour = 0; hour < 24; hour++) {
                const datetime = DateTimeValue.fromYMDHMSN(2024, 3, 15, hour, 30, 45, 123456789);
                const expected = `2024-03-15T${String(hour).padStart(2, '0')}:30:45.123456789Z`;
                expect(datetime.toString()).toBe(expected);
            }
        });
    });

    describe('display all minutes', () => {
        it('should format all minutes correctly', () => {
            for (let minute = 0; minute < 60; minute++) {
                const datetime = DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, minute, 45, 123456789);
                const expected = `2024-03-15T14:${String(minute).padStart(2, '0')}:45.123456789Z`;
                expect(datetime.toString()).toBe(expected);
            }
        });
    });

    describe('display all seconds', () => {
        it('should format all seconds correctly', () => {
            for (let second = 0; second < 60; second++) {
                const datetime = DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, second, 123456789);
                const expected = `2024-03-15T14:30:${String(second).padStart(2, '0')}.123456789Z`;
                expect(datetime.toString()).toBe(expected);
            }
        });
    });

    describe('display from timestamp', () => {
        it('should format from timestamp correctly', () => {
            expect(DateTimeValue.fromTimestamp(0).toString())
                .toBe('1970-01-01T00:00:00.000000000Z');
            expect(DateTimeValue.fromTimestamp(1234567890).toString())
                .toBe('2009-02-13T23:31:30.000000000Z');
        });
    });

    describe('display from timestamp millis', () => {
        it('should format from timestamp millis correctly', () => {
            expect(DateTimeValue.fromTimestampMillis(1234567890123).toString())
                .toBe('2009-02-13T23:31:30.123000000Z');
            expect(DateTimeValue.fromTimestampMillis(0).toString())
                .toBe('1970-01-01T00:00:00.000000000Z');
        });
    });

    describe('display from parts', () => {
        it('should format from parts correctly', () => {
            expect(DateTimeValue.fromParts(1234567890, 123456789).toString())
                .toBe('2009-02-13T23:31:30.123456789Z');
            expect(DateTimeValue.fromParts(0, 0).toString())
                .toBe('1970-01-01T00:00:00.000000000Z');
        });
    });

    describe('parse', () => {
        it('should parse valid datetime strings', () => {
            expect(DateTimeValue.parse('2024-03-15T14:30:45.123456789Z').toString())
                .toBe('2024-03-15T14:30:45.123456789Z');
            expect(DateTimeValue.parse('1970-01-01T00:00:00.000000000Z').toString())
                .toBe('1970-01-01T00:00:00.000000000Z');
            expect(DateTimeValue.parse('0001-01-01T00:00:00.000000000Z').toString())
                .toBe('0001-01-01T00:00:00.000000000Z');
        });

        it('should parse datetime without fractional seconds', () => {
            expect(DateTimeValue.parse('2024-03-15T14:30:45Z').toString())
                .toBe('2024-03-15T14:30:45.000000000Z');
        });

        it('should parse with partial fractional seconds', () => {
            expect(DateTimeValue.parse('2024-03-15T14:30:45.1Z').toString())
                .toBe('2024-03-15T14:30:45.100000000Z');
            expect(DateTimeValue.parse('2024-03-15T14:30:45.123Z').toString())
                .toBe('2024-03-15T14:30:45.123000000Z');
        });

        it('should parse with whitespace', () => {
            expect(DateTimeValue.parse('  2024-03-15T14:30:45Z  ').toString())
                .toBe('2024-03-15T14:30:45.000000000Z');
        });

        it('should return undefined for empty string', () => {
            expect(DateTimeValue.parse('').value).toBeUndefined();
            expect(DateTimeValue.parse('   ').value).toBeUndefined();
        });

        it('should return undefined for NONE_VALUE', () => {
            expect(DateTimeValue.parse('⟪none⟫').value).toBeUndefined();
        });

        it('should throw error for invalid formats', () => {
            expect(() => DateTimeValue.parse('2024-03-15 14:30:45')).toThrow('Cannot parse');
            expect(() => DateTimeValue.parse('2024-03-15T14:30:45')).toThrow('Cannot parse'); // Missing Z
            expect(() => DateTimeValue.parse('2024-03-15T14:30:45+00:00')).toThrow('Cannot parse'); // Wrong timezone format
        });

        it('should throw error for invalid datetimes', () => {
            expect(() => DateTimeValue.parse('2024-02-30T00:00:00Z')).toThrow('Cannot parse');
            expect(() => DateTimeValue.parse('2024-13-01T00:00:00Z')).toThrow('Cannot parse');
            expect(() => DateTimeValue.parse('2024-03-15T24:00:00Z')).toThrow('Cannot parse');
            expect(() => DateTimeValue.parse('2024-03-15T00:60:00Z')).toThrow('Cannot parse');
            expect(() => DateTimeValue.parse('2024-03-15T00:00:60Z')).toThrow('Cannot parse');
        });
    });

    describe('timestamp methods', () => {
        it('should get timestamp in seconds', () => {
            const datetime = DateTimeValue.fromYMDHMS(2009, 2, 13, 23, 31, 30);
            expect(datetime.timestamp()).toBe(1234567890);
        });

        it('should get timestamp in nanoseconds', () => {
            const datetime = DateTimeValue.fromYMDHMSN(2009, 2, 13, 23, 31, 30, 123456789);
            const nanos = datetime.timestampNanos()!;
            // Should be close to expected value (some precision loss due to JS Date)
            expect(nanos).toBeGreaterThan(BigInt(1234567890123000000));
            expect(nanos).toBeLessThan(BigInt(1234567890124000000));
        });

        it('should return undefined for undefined datetime', () => {
            const datetime = new DateTimeValue(undefined);
            expect(datetime.timestamp()).toBeUndefined();
            expect(datetime.timestampNanos()).toBeUndefined();
        });
    });

    describe('date and time components', () => {
        it('should extract date component', () => {
            const datetime = DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123456789);
            const date = datetime.date()!;
            expect(date.toString()).toBe('2024-03-15');
        });

        it('should extract time component', () => {
            const datetime = DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123456789);
            const time = datetime.time()!;
            expect(time.toString()).toBe('14:30:45.123456789');
        });

        it('should return undefined for undefined datetime', () => {
            const datetime = new DateTimeValue(undefined);
            expect(datetime.date()).toBeUndefined();
            expect(datetime.time()).toBeUndefined();
        });
    });

    describe('round-trip conversion', () => {
        it('should round-trip through nanoseconds', () => {
            const original = DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123456789);
            const nanos = original.toNanosSinceEpoch()!;
            const reconstructed = DateTimeValue.fromNanosSinceEpoch(nanos);
            expect(reconstructed.toString()).toBe('2024-03-15T14:30:45.123456789Z');
        });

        it('should round-trip through parts', () => {
            const original = DateTimeValue.fromYMDHMSN(2024, 3, 15, 14, 30, 45, 123456789);
            const [seconds, nanos] = original.toParts()!;
            const reconstructed = DateTimeValue.fromParts(seconds, nanos);
            expect(reconstructed.toString()).toBe('2024-03-15T14:30:45.123456789Z');
        });

        it('should round-trip through string', () => {
            const testCases = [
                '1970-01-01T00:00:00.000000000Z',
                '2024-03-15T14:30:45.123456789Z',
                '0001-01-01T00:00:00.000000000Z',
                '9999-12-31T23:59:59.999999999Z'
            ];

            testCases.forEach(str => {
                const parsed = DateTimeValue.parse(str);
                expect(parsed.toString()).toBe(str);
            });
        });
    });

    describe('valueOf', () => {
        it('should return the Date value', () => {
            const jsDate = new Date('2024-03-15T14:30:45.123Z');
            const datetime = new DateTimeValue(jsDate);
            expect(datetime.valueOf()).toBeInstanceOf(Date);
        });

        it('should return undefined when value is undefined', () => {
            const datetime = new DateTimeValue(undefined);
            expect(datetime.valueOf()).toBeUndefined();
        });
    });
});