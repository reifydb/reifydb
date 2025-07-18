/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {decodeValue} from '../../src/decoder';
import {DataType, Interval} from "../../src";

const UNDEFINED_VALUE = "⟪undefined⟫";

describe('decodeValue', () => {
    describe('undefined value handling', () => {
        it('should return undefined for ⟪undefined⟫ regardless of type', () => {
            const types: DataType[] = ['Bool', 'Float4', 'Int1', 'Utf8', 'Date', 'DateTime', 'Time', 'Interval', 'Undefined'];

            types.forEach(ty => {
                expect(decodeValue(ty, UNDEFINED_VALUE)).toBeUndefined();
            });
        });

        it('should return undefined for exact UNDEFINED_VALUE string match', () => {
            expect(decodeValue('Bool', UNDEFINED_VALUE)).toBeUndefined();
        });
    });

    describe('Bool type', () => {
        it('should return true for "true" string', () => {
            expect(decodeValue('Bool', 'true')).toBe(true);
        });

        it('should return false for "false" string', () => {
            expect(decodeValue('Bool', 'false')).toBe(false);
        });

    });

    describe('Float types (Float4, Float8)', () => {
        const floatKinds: DataType[] = ['Float4', 'Float8'];

        floatKinds.forEach(ty => {
            describe(`${ty}`, () => {
                it('should convert valid float strings to numbers', () => {
                    expect(decodeValue(ty, '3.14')).toBe(3.14);
                    expect(decodeValue(ty, '-2.5')).toBe(-2.5);
                    expect(decodeValue(ty, '0.0')).toBe(0.0);
                    expect(decodeValue(ty, '123.456')).toBe(123.456);
                });

                it('should convert integer strings to numbers', () => {
                    expect(decodeValue(ty, '42')).toBe(42);
                    expect(decodeValue(ty, '-17')).toBe(-17);
                    expect(decodeValue(ty, '0')).toBe(0);
                });

                it('should handle scientific notation', () => {
                    expect(decodeValue(ty, '1e5')).toBe(100000);
                    expect(decodeValue(ty, '2.5e-3')).toBe(0.0025);
                    expect(decodeValue(ty, '-1.23e4')).toBe(-12300);
                });
            });
        });
    });

    describe('Small singed integer types (Int1, Int2, Int4)', () => {
        const intKinds: DataType[] = ['Int1', 'Int2', 'Int4'];

        intKinds.forEach(ty => {
            describe(`${ty}`, () => {
                it('should convert valid integer strings to numbers', () => {
                    expect(decodeValue(ty, '42')).toBe(42);
                    expect(decodeValue(ty, '-17')).toBe(-17);
                    expect(decodeValue(ty, '0')).toBe(0);
                    expect(decodeValue(ty, '123')).toBe(123);
                });

                it('should handle edge cases', () => {
                    expect(decodeValue(ty, '2147483647')).toBe(2147483647);
                    expect(decodeValue(ty, '-2147483648')).toBe(-2147483648);
                });

            });
        });
    });

    describe('Small unsinged integer types (Uint1, Uint2, Uint4)', () => {
        const intKinds: DataType[] = ['Uint1', 'Uint2', 'Uint4'];

        intKinds.forEach(ty => {
            describe(`${ty}`, () => {
                it('should convert valid integer strings to numbers', () => {
                    expect(decodeValue(ty, '42')).toBe(42);
                    expect(decodeValue(ty, '0')).toBe(0);
                    expect(decodeValue(ty, '123')).toBe(123);
                });

                it('should handle edge cases', () => {
                    expect(decodeValue(ty, '2147483647')).toBe(2147483647);
                });

            });
        });
    });


    describe('big signed integer (Int8, Int16)', () => {
        const bigintKinds: DataType[] = ['Int8', 'Int16'];

        bigintKinds.forEach(ty => {
            describe(`${ty}`, () => {
                it('should convert valid integer strings to BigInt', () => {
                    expect(decodeValue(ty, '42')).toBe(BigInt(42));
                    expect(decodeValue(ty, '-17')).toBe(BigInt(-17));
                    expect(decodeValue(ty, '0')).toBe(BigInt(0));
                    expect(decodeValue(ty, '123')).toBe(BigInt(123));
                });

                it('should handle large numbers', () => {
                    expect(decodeValue(ty, '9223372036854775807')).toBe(BigInt('9223372036854775807'));
                    expect(decodeValue(ty, '-9223372036854775808')).toBe(BigInt('-9223372036854775808'));
                });

                it('should handle very large numbers', () => {
                    const largeNumber = '123456789012345678901234567890';
                    expect(decodeValue(ty, largeNumber)).toBe(BigInt(largeNumber));
                });
            });
        });
    });

    describe('big unsigned integer(Uint8, Uint16)', () => {
        const bigintKinds: DataType[] = ['Uint8', 'Uint16'];

        bigintKinds.forEach(ty => {
            describe(`${ty}`, () => {
                it('should convert valid integer strings to BigInt', () => {
                    expect(decodeValue(ty, '42')).toBe(BigInt(42));
                    expect(decodeValue(ty, '0')).toBe(BigInt(0));
                    expect(decodeValue(ty, '123')).toBe(BigInt(123));
                });

                it('should handle large numbers', () => {
                    expect(decodeValue(ty, '18446744073709551615')).toBe(BigInt('18446744073709551615'));
                });

                it('should handle very large numbers', () => {
                    const largeNumber = '123456789012345678901234567890';
                    expect(decodeValue(ty, largeNumber)).toBe(BigInt(largeNumber));
                });
            });
        });
    });

    describe('Utf8 type', () => {
        it('should return the string value as-is', () => {
            expect(decodeValue('Utf8', 'hello world')).toBe('hello world');
            expect(decodeValue('Utf8', '')).toBe('');
            expect(decodeValue('Utf8', '123')).toBe('123');
            expect(decodeValue('Utf8', 'true')).toBe('true');
            expect(decodeValue('Utf8', 'special chars: 🚀 ñ ü')).toBe('special chars: 🚀 ñ ü');
        });

        it('should handle whitespace and special characters', () => {
            expect(decodeValue('Utf8', '  spaces  ')).toBe('  spaces  ');
            expect(decodeValue('Utf8', '\n\t\r')).toBe('\n\t\r');
            expect(decodeValue('Utf8', 'line1\nline2')).toBe('line1\nline2');
        });

        it('should handle unicode characters', () => {
            expect(decodeValue('Utf8', '👨‍💻🌟')).toBe('👨‍💻🌟');
            expect(decodeValue('Utf8', 'café')).toBe('café');
            expect(decodeValue('Utf8', '中文')).toBe('中文');
        });
    });

    describe('Date type', () => {
        it('should convert date strings to Date objects', () => {
            expect(decodeValue('Date', '2024-03-15')).toEqual(new Date('2024-03-15'));
            expect(decodeValue('Date', '2000-01-01')).toEqual(new Date('2000-01-01'));
            expect(decodeValue('Date', '2024-12-31')).toEqual(new Date('2024-12-31'));
        });

        it('should handle edge cases', () => {
            expect(decodeValue('Date', '1970-01-01')).toEqual(new Date('1970-01-01'));
            expect(decodeValue('Date', '2024-02-29')).toEqual(new Date('2024-02-29')); // Leap year
        });
    });

    describe('DateTime type', () => {
        it('should convert datetime strings to Date objects', () => {
            expect(decodeValue('DateTime', '2024-03-15T14:30:00.000000000Z')).toEqual(new Date('2024-03-15T14:30:00.000000000Z'));
            expect(decodeValue('DateTime', '2000-01-01T00:00:00.000000000Z')).toEqual(new Date('2000-01-01T00:00:00.000000000Z'));
            expect(decodeValue('DateTime', '2024-12-31T23:59:59.999999999Z')).toEqual(new Date('2024-12-31T23:59:59.999999999Z'));
        });

        it('should handle different datetime formats', () => {
            expect(decodeValue('DateTime', '2024-03-15T14:30:00Z')).toEqual(new Date('2024-03-15T14:30:00Z'));
            expect(decodeValue('DateTime', '2024-03-15T14:30:00.123456789Z')).toEqual(new Date('2024-03-15T14:30:00.123456789Z'));
        });
    });

    describe('Time type', () => {
        it('should convert time strings to Date objects with today\'s date', () => {
            const result = decodeValue('Time', '14:30:00.000000000') as Date;
            expect(result).toBeInstanceOf(Date);
            expect(result.getHours()).toBe(14);
            expect(result.getMinutes()).toBe(30);
            expect(result.getSeconds()).toBe(0);
            expect(result.getMilliseconds()).toBe(0);
        });

        it('should handle time with nanoseconds', () => {
            const result = decodeValue('Time', '14:30:00.123456789') as Date;
            expect(result).toBeInstanceOf(Date);
            expect(result.getHours()).toBe(14);
            expect(result.getMinutes()).toBe(30);
            expect(result.getSeconds()).toBe(0);
            expect(result.getMilliseconds()).toBe(123); // Only milliseconds precision in JS
        });

        it('should handle edge cases', () => {
            const midnight = decodeValue('Time', '00:00:00.000000000') as Date;
            expect(midnight.getHours()).toBe(0);
            expect(midnight.getMinutes()).toBe(0);
            expect(midnight.getSeconds()).toBe(0);

            const almostMidnight = decodeValue('Time', '23:59:59.999999999') as Date;
            expect(almostMidnight.getHours()).toBe(23);
            expect(almostMidnight.getMinutes()).toBe(59);
            expect(almostMidnight.getSeconds()).toBe(59);
            expect(almostMidnight.getMilliseconds()).toBe(999);
        });
    });

    describe('Interval type', () => {
        it('should convert duration strings to Interval instances', () => {
            // 1 day = 24 * 60 * 60 * 1_000_000_000 nanos
            const interval1 = decodeValue('Interval', 'P1D') as Interval;
            expect(interval1).toBeInstanceOf(Interval);
            expect(interval1.totalNanoseconds).toBe(BigInt(24 * 60 * 60 * 1_000_000_000));
            
            // 2 hours 30 minutes = (2 * 60 * 60 + 30 * 60) * 1_000_000_000 nanos
            const interval2 = decodeValue('Interval', 'PT2H30M') as Interval;
            expect(interval2).toBeInstanceOf(Interval);
            expect(interval2.totalNanoseconds).toBe(BigInt((2 * 60 * 60 + 30 * 60) * 1_000_000_000));
            
            // 1 hour = 60 * 60 * 1_000_000_000 nanos
            const interval3 = decodeValue('Interval', 'PT1H') as Interval;
            expect(interval3).toBeInstanceOf(Interval);
            expect(interval3.totalNanoseconds).toBe(BigInt(60 * 60 * 1_000_000_000));
            
            // 30 minutes = 30 * 60 * 1_000_000_000 nanos
            const interval4 = decodeValue('Interval', 'PT30M') as Interval;
            expect(interval4).toBeInstanceOf(Interval);
            expect(interval4.totalNanoseconds).toBe(BigInt(30 * 60 * 1_000_000_000));
            
            // 45 seconds = 45 * 1_000_000_000 nanos
            const interval5 = decodeValue('Interval', 'PT45S') as Interval;
            expect(interval5).toBeInstanceOf(Interval);
            expect(interval5.totalNanoseconds).toBe(BigInt(45 * 1_000_000_000));
        });

        it('should handle complex intervals', () => {
            // 1 day + 2 hours + 30 minutes = (24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000 nanos
            const expected = BigInt((24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000);
            const interval = decodeValue('Interval', 'P1DT2H30M') as Interval;
            expect(interval).toBeInstanceOf(Interval);
            expect(interval.totalNanoseconds).toBe(expected);
        });

        it('should handle date-only intervals', () => {
            // 1 year (approximate) = 365 * 24 * 60 * 60 * 1_000_000_000 nanos
            const interval1 = decodeValue('Interval', 'P1Y') as Interval;
            expect(interval1).toBeInstanceOf(Interval);
            expect(interval1.totalNanoseconds).toBe(BigInt(365 * 24 * 60 * 60 * 1_000_000_000));
            
            // 1 month (approximate) = 30 * 24 * 60 * 60 * 1_000_000_000 nanos
            const interval2 = decodeValue('Interval', 'P1M') as Interval;
            expect(interval2).toBeInstanceOf(Interval);
            expect(interval2.totalNanoseconds).toBe(BigInt(30 * 24 * 60 * 60 * 1_000_000_000));
            
            // 1 week = 7 * 24 * 60 * 60 * 1_000_000_000 nanos
            const interval3 = decodeValue('Interval', 'P1W') as Interval;
            expect(interval3).toBeInstanceOf(Interval);
            expect(interval3.totalNanoseconds).toBe(BigInt(7 * 24 * 60 * 60 * 1_000_000_000));
        });

        it('should handle time-only intervals', () => {
            // 1 hour 30 minutes 45 seconds = (1 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000 nanos
            const expected = BigInt((60 * 60 + 30 * 60 + 45) * 1_000_000_000);
            const interval = decodeValue('Interval', 'PT1H30M45S') as Interval;
            expect(interval).toBeInstanceOf(Interval);
            expect(interval.totalNanoseconds).toBe(expected);
        });

        it('should handle edge cases', () => {
            // Complex interval with all components
            const complex = 'P1Y2M3DT4H5M6S';
            const expectedNanos = BigInt(
                365 * 24 * 60 * 60 * 1_000_000_000 +     // 1 year
                2 * 30 * 24 * 60 * 60 * 1_000_000_000 +  // 2 months
                3 * 24 * 60 * 60 * 1_000_000_000 +       // 3 days
                4 * 60 * 60 * 1_000_000_000 +            // 4 hours
                5 * 60 * 1_000_000_000 +                 // 5 minutes
                6 * 1_000_000_000                        // 6 seconds
            );
            const interval = decodeValue('Interval', complex) as Interval;
            expect(interval).toBeInstanceOf(Interval);
            expect(interval.totalNanoseconds).toBe(expectedNanos);
        });

        it('should throw error for invalid interval format', () => {
            expect(() => decodeValue('Interval', 'invalid')).toThrow('Invalid interval format - must start with P');
            expect(() => decodeValue('Interval', 'P1X')).toThrow('Invalid character in interval: X');
            expect(() => decodeValue('Interval', 'P1TY')).toThrow('Years not allowed in time part');
            expect(() => decodeValue('Interval', 'P1TW')).toThrow('Weeks not allowed in time part');
            expect(() => decodeValue('Interval', 'P1TD')).toThrow('Days not allowed in time part');
            expect(() => decodeValue('Interval', 'P1H')).toThrow('Hours only allowed in time part');
            expect(() => decodeValue('Interval', 'P1S')).toThrow('Seconds only allowed in time part');
        });

        it('should provide useful interval methods', () => {
            const interval = decodeValue('Interval', 'P1DT2H30M45S') as Interval;
            expect(interval).toBeInstanceOf(Interval);
            
            // Test components
            const components = interval.components;
            expect(components.days).toBe(BigInt(1));
            expect(components.hours).toBe(BigInt(2));
            expect(components.minutes).toBe(BigInt(30));
            expect(components.seconds).toBe(BigInt(45));
            
            // Test toString
            expect(interval.toString()).toBe('P1DT2H30M45S');
            
            // Test totals
            expect(interval.totalDays).toBe(BigInt(1));
            expect(interval.totalHours).toBe(BigInt(26)); // 1 day + 2 hours
            expect(interval.totalMinutes).toBe(BigInt(1590)); // 26 hours + 30 minutes
            expect(interval.totalSeconds).toBe(BigInt(95445)); // 1590 minutes + 45 seconds
        });
    });

    describe('Undefined type', () => {
        it('should always return undefined regardless of value', () => {
            expect(decodeValue('Undefined', 'anything')).toBeUndefined();
            expect(decodeValue('Undefined', 'true')).toBeUndefined();
            expect(decodeValue('Undefined', '123')).toBeUndefined();
            expect(decodeValue('Undefined', '')).toBeUndefined();
            expect(decodeValue('Undefined', UNDEFINED_VALUE)).toBeUndefined();
        });
    });

    describe('unknown type', () => {
        it('should throw an error for unknown tys', () => {
            // @ts-expect-error - Testing invalid ty
            expect(() => decodeValue('InvalidKind', 'value')).toThrow('Unknown data type: InvalidKind');

            // @ts-expect-error - Testing invalid ty
            expect(() => decodeValue('String', 'value')).toThrow('Unknown data type: String');

            // @ts-expect-error - Testing invalid ty
            expect(() => decodeValue('', 'value')).toThrow('Unknown data type: ');
        });
    });
});