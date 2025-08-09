/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { describe, expect, it } from 'vitest';
import { IntervalValue } from '../../src/value/interval';

describe('IntervalValue', () => {
    describe('display zero', () => {
        it('should format zero interval', () => {
            expect(IntervalValue.zero().toString()).toBe('PT0S');
            expect(IntervalValue.fromSeconds(0).toString()).toBe('PT0S');
            expect(IntervalValue.fromNanoseconds(0n).toString()).toBe('PT0S');
            expect(IntervalValue.default().toString()).toBe('PT0S');
        });
    });

    describe('display seconds only', () => {
        it('should format seconds correctly', () => {
            expect(IntervalValue.fromSeconds(1).toString()).toBe('PT1S');
            expect(IntervalValue.fromSeconds(30).toString()).toBe('PT30S');
            expect(IntervalValue.fromSeconds(59).toString()).toBe('PT59S');
        });
    });

    describe('display minutes only', () => {
        it('should format minutes correctly', () => {
            expect(IntervalValue.fromMinutes(1).toString()).toBe('PT1M');
            expect(IntervalValue.fromMinutes(30).toString()).toBe('PT30M');
            expect(IntervalValue.fromMinutes(59).toString()).toBe('PT59M');
        });
    });

    describe('display hours only', () => {
        it('should format hours correctly', () => {
            expect(IntervalValue.fromHours(1).toString()).toBe('PT1H');
            expect(IntervalValue.fromHours(12).toString()).toBe('PT12H');
            expect(IntervalValue.fromHours(23).toString()).toBe('PT23H');
        });
    });

    describe('display days only', () => {
        it('should format days correctly', () => {
            expect(IntervalValue.fromDays(1).toString()).toBe('P1D');
            expect(IntervalValue.fromDays(7).toString()).toBe('P7D');
            expect(IntervalValue.fromDays(365).toString()).toBe('P365D');
        });
    });

    describe('display weeks only', () => {
        it('should format weeks correctly', () => {
            expect(IntervalValue.fromWeeks(1).toString()).toBe('P7D');
            expect(IntervalValue.fromWeeks(2).toString()).toBe('P14D');
            expect(IntervalValue.fromWeeks(52).toString()).toBe('P364D');
        });
    });

    describe('display months only', () => {
        it('should format months correctly', () => {
            expect(IntervalValue.fromMonths(1).toString()).toBe('P1M');
            expect(IntervalValue.fromMonths(6).toString()).toBe('P6M');
            expect(IntervalValue.fromMonths(11).toString()).toBe('P11M');
        });
    });

    describe('display years only', () => {
        it('should format years correctly', () => {
            expect(IntervalValue.fromYears(1).toString()).toBe('P1Y');
            expect(IntervalValue.fromYears(10).toString()).toBe('P10Y');
            expect(IntervalValue.fromYears(100).toString()).toBe('P100Y');
        });
    });

    describe('display combined time', () => {
        it('should format hours and minutes', () => {
            const interval = IntervalValue.new(0, 0, (1n * 60n * 60n + 30n * 60n) * 1_000_000_000n);
            expect(interval.toString()).toBe('PT1H30M');
        });

        it('should format minutes and seconds', () => {
            const interval = IntervalValue.new(0, 0, (5n * 60n + 45n) * 1_000_000_000n);
            expect(interval.toString()).toBe('PT5M45S');
        });

        it('should format hours, minutes, and seconds', () => {
            const interval = IntervalValue.new(0, 0, (2n * 60n * 60n + 30n * 60n + 45n) * 1_000_000_000n);
            expect(interval.toString()).toBe('PT2H30M45S');
        });
    });

    describe('display combined date time', () => {
        it('should format days and hours', () => {
            const interval = IntervalValue.new(0, 1, 2n * 60n * 60n * 1_000_000_000n);
            expect(interval.toString()).toBe('P1DT2H');
        });

        it('should format days and minutes', () => {
            const interval = IntervalValue.new(0, 1, 30n * 60n * 1_000_000_000n);
            expect(interval.toString()).toBe('P1DT30M');
        });

        it('should format days, hours, and minutes', () => {
            const interval = IntervalValue.new(0, 1, (2n * 60n * 60n + 30n * 60n) * 1_000_000_000n);
            expect(interval.toString()).toBe('P1DT2H30M');
        });

        it('should format days, hours, minutes, and seconds', () => {
            const interval = IntervalValue.new(0, 1, (2n * 60n * 60n + 30n * 60n + 45n) * 1_000_000_000n);
            expect(interval.toString()).toBe('P1DT2H30M45S');
        });
    });

    describe('display years and months', () => {
        it('should format years and months', () => {
            const interval = IntervalValue.new(13, 0, 0n); // 1 year 1 month
            expect(interval.toString()).toBe('P1Y1M');
        });

        it('should format multiple years and months', () => {
            const interval = IntervalValue.new(27, 0, 0n); // 2 years 3 months
            expect(interval.toString()).toBe('P2Y3M');
        });
    });

    describe('display milliseconds', () => {
        it('should format milliseconds correctly', () => {
            expect(IntervalValue.fromMilliseconds(123).toString()).toBe('PT0.123S');
            expect(IntervalValue.fromMilliseconds(1).toString()).toBe('PT0.001S');
            expect(IntervalValue.fromMilliseconds(999).toString()).toBe('PT0.999S');
            expect(IntervalValue.fromMilliseconds(1500).toString()).toBe('PT1.5S');
        });
    });

    describe('display microseconds', () => {
        it('should format microseconds correctly', () => {
            expect(IntervalValue.fromMicroseconds(123456).toString()).toBe('PT0.123456S');
            expect(IntervalValue.fromMicroseconds(1).toString()).toBe('PT0.000001S');
            expect(IntervalValue.fromMicroseconds(999999).toString()).toBe('PT0.999999S');
            expect(IntervalValue.fromMicroseconds(1500000).toString()).toBe('PT1.5S');
        });
    });

    describe('display nanoseconds', () => {
        it('should format nanoseconds correctly', () => {
            expect(IntervalValue.fromNanoseconds(123456789n).toString()).toBe('PT0.123456789S');
            expect(IntervalValue.fromNanoseconds(1n).toString()).toBe('PT0.000000001S');
            expect(IntervalValue.fromNanoseconds(999999999n).toString()).toBe('PT0.999999999S');
            expect(IntervalValue.fromNanoseconds(1500000000n).toString()).toBe('PT1.5S');
        });
    });

    describe('display fractional seconds with integers', () => {
        it('should format seconds with milliseconds', () => {
            const interval = IntervalValue.new(0, 0, 1n * 1_000_000_000n + 500n * 1_000_000n);
            expect(interval.toString()).toBe('PT1.5S');
        });

        it('should format seconds with microseconds', () => {
            const interval = IntervalValue.new(0, 0, 2n * 1_000_000_000n + 123456n * 1_000n);
            expect(interval.toString()).toBe('PT2.123456S');
        });

        it('should format seconds with nanoseconds', () => {
            const interval = IntervalValue.new(0, 0, 3n * 1_000_000_000n + 123456789n);
            expect(interval.toString()).toBe('PT3.123456789S');
        });
    });

    describe('display complex intervals', () => {
        it('should format complex interval with all components', () => {
            const interval = IntervalValue.new(
                0, 
                1, 
                (2n * 60n * 60n + 30n * 60n + 45n) * 1_000_000_000n + 123n * 1_000_000n
            );
            expect(interval.toString()).toBe('P1DT2H30M45.123S');
        });

        it('should format another complex interval', () => {
            const interval = IntervalValue.new(
                0,
                7,
                (12n * 60n * 60n + 45n * 60n + 30n) * 1_000_000_000n + 456789n * 1_000n
            );
            expect(interval.toString()).toBe('P7DT12H45M30.456789S');
        });
    });

    describe('display trailing zeros removed', () => {
        it('should remove trailing zeros from fractional seconds', () => {
            expect(IntervalValue.fromNanoseconds(100000000n).toString()).toBe('PT0.1S');
            expect(IntervalValue.fromNanoseconds(120000000n).toString()).toBe('PT0.12S');
            expect(IntervalValue.fromNanoseconds(123000000n).toString()).toBe('PT0.123S');
            expect(IntervalValue.fromNanoseconds(123400000n).toString()).toBe('PT0.1234S');
            expect(IntervalValue.fromNanoseconds(123450000n).toString()).toBe('PT0.12345S');
            expect(IntervalValue.fromNanoseconds(123456000n).toString()).toBe('PT0.123456S');
            expect(IntervalValue.fromNanoseconds(123456700n).toString()).toBe('PT0.1234567S');
            expect(IntervalValue.fromNanoseconds(123456780n).toString()).toBe('PT0.12345678S');
            expect(IntervalValue.fromNanoseconds(123456789n).toString()).toBe('PT0.123456789S');
        });
    });

    describe('display negative intervals', () => {
        it('should format negative intervals', () => {
            expect(IntervalValue.fromSeconds(-30).toString()).toBe('PT-30S');
            expect(IntervalValue.fromMinutes(-5).toString()).toBe('PT-5M');
            expect(IntervalValue.fromHours(-2).toString()).toBe('PT-2H');
            expect(IntervalValue.fromDays(-1).toString()).toBe('P-1D');
        });
    });

    describe('display large values', () => {
        it('should format large intervals', () => {
            expect(IntervalValue.fromDays(1000).toString()).toBe('P1000D');
            expect(IntervalValue.fromHours(25).toString()).toBe('P1DT1H');
            expect(IntervalValue.fromMinutes(1500).toString()).toBe('P1DT1H'); // 25 hours
            expect(IntervalValue.fromSeconds(90000).toString()).toBe('P1DT1H'); // 25 hours
        });
    });

    describe('display edge cases', () => {
        it('should format edge cases', () => {
            expect(IntervalValue.fromNanoseconds(1n).toString()).toBe('PT0.000000001S');
            expect(IntervalValue.fromNanoseconds(999999999n).toString()).toBe('PT0.999999999S');
            expect(IntervalValue.fromNanoseconds(1000000000n).toString()).toBe('PT1S');
            expect(IntervalValue.fromNanoseconds(60n * 1000000000n).toString()).toBe('PT1M');
            expect(IntervalValue.fromNanoseconds(3600n * 1000000000n).toString()).toBe('PT1H');
            expect(IntervalValue.fromNanoseconds(86400n * 1000000000n).toString()).toBe('P1D');
        });
    });

    describe('display precision boundaries', () => {
        it('should format precision boundaries', () => {
            expect(IntervalValue.fromNanoseconds(100n).toString()).toBe('PT0.0000001S');
            expect(IntervalValue.fromNanoseconds(10n).toString()).toBe('PT0.00000001S');
            expect(IntervalValue.fromNanoseconds(1n).toString()).toBe('PT0.000000001S');
        });
    });

    describe('display from nanos', () => {
        it('should format from nanoseconds', () => {
            expect(IntervalValue.fromNanoseconds(123456789n).toString()).toBe('PT0.123456789S');
            expect(IntervalValue.fromNanoseconds(3661000000000n).toString()).toBe('PT1H1M1S'); // 1 hour 1 minute 1 second
        });
    });

    describe('abs and negate', () => {
        it('should calculate absolute value', () => {
            const interval = IntervalValue.fromSeconds(-30);
            const absInterval = interval.abs();
            expect(absInterval.toString()).toBe('PT30S');
        });

        it('should negate interval', () => {
            const interval = IntervalValue.fromSeconds(30);
            const negInterval = interval.negate();
            expect(negInterval.toString()).toBe('PT-30S');
        });
    });

    describe('parse', () => {
        it('should parse valid duration strings', () => {
            expect(IntervalValue.parse('PT0S').toString()).toBe('PT0S');
            expect(IntervalValue.parse('PT1S').toString()).toBe('PT1S');
            expect(IntervalValue.parse('PT1M').toString()).toBe('PT1M');
            expect(IntervalValue.parse('PT1H').toString()).toBe('PT1H');
            expect(IntervalValue.parse('P1D').toString()).toBe('P1D');
            expect(IntervalValue.parse('P1M').toString()).toBe('P1M');
            expect(IntervalValue.parse('P1Y').toString()).toBe('P1Y');
        });

        it('should parse complex duration strings', () => {
            expect(IntervalValue.parse('P1Y2M3DT4H5M6S').toString()).toBe('P1Y2M3DT4H5M6S');
            expect(IntervalValue.parse('PT1H30M').toString()).toBe('PT1H30M');
            expect(IntervalValue.parse('P7DT12H').toString()).toBe('P7DT12H');
        });

        it('should parse fractional seconds', () => {
            expect(IntervalValue.parse('PT0.123S').toString()).toBe('PT0.123S');
            expect(IntervalValue.parse('PT1.5S').toString()).toBe('PT1.5S');
            expect(IntervalValue.parse('PT0.123456789S').toString()).toBe('PT0.123456789S');
        });

        it('should parse negative durations', () => {
            expect(IntervalValue.parse('-PT30S').toString()).toBe('PT-30S');
            expect(IntervalValue.parse('-P1D').toString()).toBe('P-1D');
        });

        it('should return undefined for empty string', () => {
            expect(IntervalValue.parse('').value).toBeUndefined();
            expect(IntervalValue.parse('   ').value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            expect(IntervalValue.parse('⟪undefined⟫').value).toBeUndefined();
        });

        it('should throw error for invalid formats', () => {
            expect(() => IntervalValue.parse('invalid')).toThrow('Cannot parse');
            expect(() => IntervalValue.parse('30S')).toThrow('Cannot parse'); // Missing P
            expect(() => IntervalValue.parse('P')).toThrow('Cannot parse'); // Missing values
        });
    });

    describe('component accessors', () => {
        it('should return correct components', () => {
            const interval = IntervalValue.new(13, 7, 123456789n);
            expect(interval.getMonths()).toBe(13);
            expect(interval.getDays()).toBe(7);
            expect(interval.getNanos()).toBe(123456789n);
        });

        it('should return undefined for undefined interval', () => {
            const interval = new IntervalValue(undefined);
            expect(interval.getMonths()).toBeUndefined();
            expect(interval.getDays()).toBeUndefined();
            expect(interval.getNanos()).toBeUndefined();
        });
    });

    describe('conversion methods', () => {
        it('should convert to seconds', () => {
            const interval = IntervalValue.fromSeconds(30);
            expect(interval.seconds()).toBe(30n);
        });

        it('should convert to milliseconds', () => {
            const interval = IntervalValue.fromMilliseconds(1500);
            expect(interval.milliseconds()).toBe(1500n);
        });

        it('should convert to microseconds', () => {
            const interval = IntervalValue.fromMicroseconds(1500000);
            expect(interval.microseconds()).toBe(1500000n);
        });

        it('should convert to nanoseconds', () => {
            const interval = IntervalValue.fromNanoseconds(1500000000n);
            expect(interval.nanoseconds()).toBe(1500000000n);
        });
    });

    describe('isPositive and isNegative', () => {
        it('should detect positive intervals', () => {
            expect(IntervalValue.fromSeconds(1).isPositive()).toBe(true);
            expect(IntervalValue.fromDays(1).isPositive()).toBe(true);
            expect(IntervalValue.fromMonths(1).isPositive()).toBe(true);
        });

        it('should detect negative intervals', () => {
            expect(IntervalValue.fromSeconds(-1).isNegative()).toBe(true);
            expect(IntervalValue.fromDays(-1).isNegative()).toBe(true);
            expect(IntervalValue.fromMonths(-1).isNegative()).toBe(true);
        });

        it('should handle zero interval', () => {
            const zero = IntervalValue.zero();
            expect(zero.isPositive()).toBe(false);
            expect(zero.isNegative()).toBe(false);
        });

        it('should handle undefined interval', () => {
            const undef = new IntervalValue(undefined);
            expect(undef.isPositive()).toBe(false);
            expect(undef.isNegative()).toBe(false);
        });
    });

    describe('valueOf', () => {
        it('should return the internal value', () => {
            const interval = IntervalValue.new(1, 2, 3n);
            const value = interval.valueOf();
            expect(value).toEqual({ months: 1, days: 2, nanos: 3n });
        });

        it('should return undefined when value is undefined', () => {
            const interval = new IntervalValue(undefined);
            expect(interval.valueOf()).toBeUndefined();
        });
    });
});