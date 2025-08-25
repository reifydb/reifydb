/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { describe, expect, it } from 'vitest';
import { TimeValue } from '../../src';

describe('TimeValue', () => {
    describe('constructor', () => {
        it('should create instance with bigint nanoseconds', () => {
            const time = new TimeValue(0n);
            expect(time.value).toBe(0n);
            expect(time.type).toBe('Time');
        });

        it('should create instance with number nanoseconds', () => {
            const time = new TimeValue(1000000000);
            expect(time.value).toBe(1000000000n);
        });

        it('should create instance with time string', () => {
            const time = new TimeValue('14:30:45.123456789');
            expect(time.toString()).toBe('14:30:45.123456789');
        });

        it('should create instance with undefined', () => {
            const time = new TimeValue(undefined);
            expect(time.value).toBeUndefined();
            expect(time.toString()).toBe('undefined');
        });

        it('should throw error for negative nanoseconds', () => {
            expect(() => new TimeValue(-1n)).toThrow('Time value must be between 0 and');
        });

        it('should throw error for nanoseconds >= 24 hours', () => {
            expect(() => new TimeValue(86_400_000_000_000n)).toThrow('Time value must be between 0 and');
        });

        it('should throw error for invalid time string', () => {
            expect(() => new TimeValue('invalid')).toThrow('Invalid time string: invalid');
            expect(() => new TimeValue('25:00:00')).toThrow('Invalid time string: 25:00:00');
        });
    });

    describe('fromHMSN', () => {
        it('should create time from hour, minute, second, nano', () => {
            const time = TimeValue.fromHMSN(14, 30, 45, 123456789);
            expect(time.hour()).toBe(14);
            expect(time.minute()).toBe(30);
            expect(time.second()).toBe(45);
            expect(time.nanosecond()).toBe(123456789);
        });

        it('should default nano to 0', () => {
            const time = TimeValue.fromHMSN(14, 30, 45);
            expect(time.nanosecond()).toBe(0);
        });

        it('should throw error for invalid hour', () => {
            expect(() => TimeValue.fromHMSN(24, 0, 0)).toThrow('Invalid hour: 24');
            expect(() => TimeValue.fromHMSN(-1, 0, 0)).toThrow('Invalid hour: -1');
        });

        it('should throw error for invalid minute', () => {
            expect(() => TimeValue.fromHMSN(0, 60, 0)).toThrow('Invalid minute: 60');
            expect(() => TimeValue.fromHMSN(0, -1, 0)).toThrow('Invalid minute: -1');
        });

        it('should throw error for invalid second', () => {
            expect(() => TimeValue.fromHMSN(0, 0, 60)).toThrow('Invalid second: 60');
            expect(() => TimeValue.fromHMSN(0, 0, -1)).toThrow('Invalid second: -1');
        });

        it('should throw error for invalid nanosecond', () => {
            expect(() => TimeValue.fromHMSN(0, 0, 0, 1_000_000_000)).toThrow('Invalid nanosecond: 1000000000');
            expect(() => TimeValue.fromHMSN(0, 0, 0, -1)).toThrow('Invalid nanosecond: -1');
        });
    });

    describe('fromHMS', () => {
        it('should create time without fractional seconds', () => {
            const time = TimeValue.fromHMS(14, 30, 45);
            expect(time.toString()).toBe('14:30:45.000000000');
        });
    });

    describe('display standard format', () => {
        it('should format standard times correctly', () => {
            expect(TimeValue.fromHMSN(14, 30, 45, 123456789).toString()).toBe('14:30:45.123456789');
            expect(TimeValue.fromHMSN(0, 0, 0, 0).toString()).toBe('00:00:00.000000000');
            expect(TimeValue.fromHMSN(23, 59, 59, 999999999).toString()).toBe('23:59:59.999999999');
        });
    });

    describe('display millisecond precision', () => {
        it('should format millisecond values correctly', () => {
            expect(TimeValue.fromHMSN(14, 30, 45, 123000000).toString()).toBe('14:30:45.123000000');
            expect(TimeValue.fromHMSN(14, 30, 45, 1000000).toString()).toBe('14:30:45.001000000');
            expect(TimeValue.fromHMSN(14, 30, 45, 999000000).toString()).toBe('14:30:45.999000000');
        });
    });

    describe('display microsecond precision', () => {
        it('should format microsecond values correctly', () => {
            expect(TimeValue.fromHMSN(14, 30, 45, 123456000).toString()).toBe('14:30:45.123456000');
            expect(TimeValue.fromHMSN(14, 30, 45, 1000).toString()).toBe('14:30:45.000001000');
            expect(TimeValue.fromHMSN(14, 30, 45, 999999000).toString()).toBe('14:30:45.999999000');
        });
    });

    describe('display nanosecond precision', () => {
        it('should format nanosecond values correctly', () => {
            expect(TimeValue.fromHMSN(14, 30, 45, 123456789).toString()).toBe('14:30:45.123456789');
            expect(TimeValue.fromHMSN(14, 30, 45, 1).toString()).toBe('14:30:45.000000001');
            expect(TimeValue.fromHMSN(14, 30, 45, 999999999).toString()).toBe('14:30:45.999999999');
        });
    });

    describe('display zero fractional seconds', () => {
        it('should format zero fractional seconds', () => {
            expect(TimeValue.fromHMSN(14, 30, 45, 0).toString()).toBe('14:30:45.000000000');
            expect(TimeValue.fromHMSN(0, 0, 0, 0).toString()).toBe('00:00:00.000000000');
        });
    });

    describe('display edge times', () => {
        it('should format edge times correctly', () => {
            // Midnight
            expect(TimeValue.fromHMSN(0, 0, 0, 0).toString()).toBe('00:00:00.000000000');
            // Almost midnight next day
            expect(TimeValue.fromHMSN(23, 59, 59, 999999999).toString()).toBe('23:59:59.999999999');
            // Noon
            expect(TimeValue.fromHMSN(12, 0, 0, 0).toString()).toBe('12:00:00.000000000');
            // One second before midnight
            expect(TimeValue.fromHMSN(23, 59, 58, 999999999).toString()).toBe('23:59:58.999999999');
            // One second after midnight
            expect(TimeValue.fromHMSN(0, 0, 1, 0).toString()).toBe('00:00:01.000000000');
        });
    });

    describe('display special times', () => {
        it('should format midnight and noon correctly', () => {
            expect(TimeValue.midnight().toString()).toBe('00:00:00.000000000');
            expect(TimeValue.noon().toString()).toBe('12:00:00.000000000');
        });
    });

    describe('display all hours', () => {
        it('should format all hours correctly', () => {
            for (let hour = 0; hour < 24; hour++) {
                const time = TimeValue.fromHMSN(hour, 30, 45, 123456789);
                const expected = `${String(hour).padStart(2, '0')}:30:45.123456789`;
                expect(time.toString()).toBe(expected);
            }
        });
    });

    describe('display all minutes', () => {
        it('should format all minutes correctly', () => {
            for (let minute = 0; minute < 60; minute++) {
                const time = TimeValue.fromHMSN(14, minute, 45, 123456789);
                const expected = `14:${String(minute).padStart(2, '0')}:45.123456789`;
                expect(time.toString()).toBe(expected);
            }
        });
    });

    describe('display all seconds', () => {
        it('should format all seconds correctly', () => {
            for (let second = 0; second < 60; second++) {
                const time = TimeValue.fromHMSN(14, 30, second, 123456789);
                const expected = `14:30:${String(second).padStart(2, '0')}.123456789`;
                expect(time.toString()).toBe(expected);
            }
        });
    });

    describe('display from_hms', () => {
        it('should format times created with fromHMS', () => {
            expect(TimeValue.fromHMS(14, 30, 45).toString()).toBe('14:30:45.000000000');
            expect(TimeValue.fromHMS(0, 0, 0).toString()).toBe('00:00:00.000000000');
            expect(TimeValue.fromHMS(23, 59, 59).toString()).toBe('23:59:59.000000000');
        });
    });

    describe('display from_nanos_since_midnight', () => {
        it('should format times from nanoseconds correctly', () => {
            // Test midnight
            expect(TimeValue.fromNanosSinceMidnight(0n).toString()).toBe('00:00:00.000000000');
            
            // Test 1 second
            expect(TimeValue.fromNanosSinceMidnight(1_000_000_000n).toString()).toBe('00:00:01.000000000');
            
            // Test 1 minute
            expect(TimeValue.fromNanosSinceMidnight(60_000_000_000n).toString()).toBe('00:01:00.000000000');
            
            // Test 1 hour
            expect(TimeValue.fromNanosSinceMidnight(3_600_000_000_000n).toString()).toBe('01:00:00.000000000');
            
            // Test comptokenize time with nanoseconds
            const nanos = 14n * 3600n * 1_000_000_000n + 
                         30n * 60n * 1_000_000_000n + 
                         45n * 1_000_000_000n + 
                         123456789n;
            expect(TimeValue.fromNanosSinceMidnight(nanos).toString()).toBe('14:30:45.123456789');
        });
    });

    describe('display boundary values', () => {
        it('should format boundary values correctly', () => {
            // Test the very last nanosecond of the day
            const nanos = 24n * 3600n * 1_000_000_000n - 1n;
            expect(TimeValue.fromNanosSinceMidnight(nanos).toString()).toBe('23:59:59.999999999');
            
            // Test the very first nanosecond of the day
            expect(TimeValue.fromNanosSinceMidnight(1n).toString()).toBe('00:00:00.000000001');
        });
    });

    describe('display precision patterns', () => {
        it('should format different precision patterns correctly', () => {
            // 0.1 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 100000000).toString()).toBe('14:30:45.100000000');
            // 0.01 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 10000000).toString()).toBe('14:30:45.010000000');
            // 0.001 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 1000000).toString()).toBe('14:30:45.001000000');
            // 0.0001 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 100000).toString()).toBe('14:30:45.000100000');
            // 0.00001 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 10000).toString()).toBe('14:30:45.000010000');
            // 0.000001 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 1000).toString()).toBe('14:30:45.000001000');
            // 0.0000001 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 100).toString()).toBe('14:30:45.000000100');
            // 0.00000001 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 10).toString()).toBe('14:30:45.000000010');
            // 0.000000001 seconds
            expect(TimeValue.fromHMSN(14, 30, 45, 1).toString()).toBe('14:30:45.000000001');
        });
    });

    describe('parse', () => {
        it('should parse valid time strings', () => {
            expect(TimeValue.parse('14:30:45.123456789').toString()).toBe('14:30:45.123456789');
            expect(TimeValue.parse('00:00:00.000000000').toString()).toBe('00:00:00.000000000');
            expect(TimeValue.parse('23:59:59.999999999').toString()).toBe('23:59:59.999999999');
        });

        it('should parse time without fractional seconds', () => {
            expect(TimeValue.parse('14:30:45').toString()).toBe('14:30:45.000000000');
            expect(TimeValue.parse('00:00:00').toString()).toBe('00:00:00.000000000');
        });

        it('should parse with partial fractional seconds', () => {
            expect(TimeValue.parse('14:30:45.1').toString()).toBe('14:30:45.100000000');
            expect(TimeValue.parse('14:30:45.12').toString()).toBe('14:30:45.120000000');
            expect(TimeValue.parse('14:30:45.123').toString()).toBe('14:30:45.123000000');
        });

        it('should parse with whitespace', () => {
            expect(TimeValue.parse('  14:30:45  ').toString()).toBe('14:30:45.000000000');
        });

        it('should return undefined for empty string', () => {
            expect(TimeValue.parse('').value).toBeUndefined();
            expect(TimeValue.parse('   ').value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            expect(TimeValue.parse('⟪undefined⟫').value).toBeUndefined();
        });

        it('should throw error for invalid formats', () => {
            expect(() => TimeValue.parse('14:30')).toThrow('Cannot parse "14:30" as Time');
            expect(() => TimeValue.parse('2:30:45')).toThrow('Cannot parse "2:30:45" as Time'); // Requires padding
            expect(() => TimeValue.parse('14:30:45 PM')).toThrow('Cannot parse "14:30:45 PM" as Time');
        });

        it('should throw error for invalid times', () => {
            expect(() => TimeValue.parse('24:00:00')).toThrow('Cannot parse "24:00:00" as Time');
            expect(() => TimeValue.parse('00:60:00')).toThrow('Cannot parse "00:60:00" as Time');
            expect(() => TimeValue.parse('00:00:60')).toThrow('Cannot parse "00:00:60" as Time');
        });
    });

    describe('component accessors', () => {
        it('should return correct hour, minute, second, nanosecond', () => {
            const time = TimeValue.fromHMSN(14, 30, 45, 123456789);
            expect(time.hour()).toBe(14);
            expect(time.minute()).toBe(30);
            expect(time.second()).toBe(45);
            expect(time.nanosecond()).toBe(123456789);
        });

        it('should return undefined for undefined time', () => {
            const time = new TimeValue(undefined);
            expect(time.hour()).toBeUndefined();
            expect(time.minute()).toBeUndefined();
            expect(time.second()).toBeUndefined();
            expect(time.nanosecond()).toBeUndefined();
        });
    });

    describe('toNanosSinceMidnight and fromNanosSinceMidnight', () => {
        it('should convert to and from nanoseconds', () => {
            const time = TimeValue.fromHMSN(14, 30, 45, 123456789);
            const nanos = time.toNanosSinceMidnight()!;
            const reconstructed = TimeValue.fromNanosSinceMidnight(nanos);
            expect(reconstructed.toString()).toBe('14:30:45.123456789');
        });

        it('should handle midnight', () => {
            const time = TimeValue.midnight();
            expect(time.toNanosSinceMidnight()).toBe(0n);
        });

        it('should handle last nanosecond of day', () => {
            const time = TimeValue.fromHMSN(23, 59, 59, 999999999);
            expect(time.toNanosSinceMidnight()).toBe(86_399_999_999_999n);
        });

        it('should return undefined for undefined value', () => {
            const time = new TimeValue(undefined);
            expect(time.toNanosSinceMidnight()).toBeUndefined();
        });

        it('should round-trip correctly', () => {
            const testTimes = [
                '00:00:00.000000000',
                '14:30:45.123456789',
                '23:59:59.999999999',
                '12:00:00.000000000',
                '00:00:00.000000001'
            ];

            testTimes.forEach(timeStr => {
                const original = TimeValue.parse(timeStr);
                const nanos = original.toNanosSinceMidnight()!;
                const reconstructed = TimeValue.fromNanosSinceMidnight(nanos);
                expect(reconstructed.toString()).toBe(timeStr);
            });
        });
    });

    describe('valueOf', () => {
        it('should return the bigint value', () => {
            const time = TimeValue.fromHMSN(0, 0, 1, 0);
            expect(time.valueOf()).toBe(1_000_000_000n);
        });

        it('should return undefined when value is undefined', () => {
            const time = new TimeValue(undefined);
            expect(time.valueOf()).toBeUndefined();
        });
    });
});