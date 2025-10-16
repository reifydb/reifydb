/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { describe, expect, it } from 'vitest';
import { DurationValue } from '../../src';

describe('DurationValue', () => {
    describe('display zero', () => {
        it('should format zero duration', () => {
            expect(DurationValue.zero().toString()).toBe('PT0S');
            expect(DurationValue.fromSeconds(0).toString()).toBe('PT0S');
            expect(DurationValue.fromNanoseconds(0n).toString()).toBe('PT0S');
            expect(DurationValue.default().toString()).toBe('PT0S');
        });
    });

    describe('display seconds only', () => {
        it('should format seconds correctly', () => {
            expect(DurationValue.fromSeconds(1).toString()).toBe('PT1S');
            expect(DurationValue.fromSeconds(30).toString()).toBe('PT30S');
            expect(DurationValue.fromSeconds(59).toString()).toBe('PT59S');
        });
    });

    describe('display minutes only', () => {
        it('should format minutes correctly', () => {
            expect(DurationValue.fromMinutes(1).toString()).toBe('PT1M');
            expect(DurationValue.fromMinutes(30).toString()).toBe('PT30M');
            expect(DurationValue.fromMinutes(59).toString()).toBe('PT59M');
        });
    });

    describe('display hours only', () => {
        it('should format hours correctly', () => {
            expect(DurationValue.fromHours(1).toString()).toBe('PT1H');
            expect(DurationValue.fromHours(12).toString()).toBe('PT12H');
            expect(DurationValue.fromHours(23).toString()).toBe('PT23H');
        });
    });

    describe('display days only', () => {
        it('should format days correctly', () => {
            expect(DurationValue.fromDays(1).toString()).toBe('P1D');
            expect(DurationValue.fromDays(7).toString()).toBe('P7D');
            expect(DurationValue.fromDays(365).toString()).toBe('P365D');
        });
    });

    describe('display weeks only', () => {
        it('should format weeks correctly', () => {
            expect(DurationValue.fromWeeks(1).toString()).toBe('P7D');
            expect(DurationValue.fromWeeks(2).toString()).toBe('P14D');
            expect(DurationValue.fromWeeks(52).toString()).toBe('P364D');
        });
    });

    describe('display months only', () => {
        it('should format months correctly', () => {
            expect(DurationValue.fromMonths(1).toString()).toBe('P1M');
            expect(DurationValue.fromMonths(6).toString()).toBe('P6M');
            expect(DurationValue.fromMonths(11).toString()).toBe('P11M');
        });
    });

    describe('display years only', () => {
        it('should format years correctly', () => {
            expect(DurationValue.fromYears(1).toString()).toBe('P1Y');
            expect(DurationValue.fromYears(10).toString()).toBe('P10Y');
            expect(DurationValue.fromYears(100).toString()).toBe('P100Y');
        });
    });

    describe('display combined time', () => {
        it('should format hours and minutes', () => {
            const duration = DurationValue.new(0, 0, (1n * 60n * 60n + 30n * 60n) * 1_000_000_000n);
            expect(duration.toString()).toBe('PT1H30M');
        });

        it('should format minutes and seconds', () => {
            const duration = DurationValue.new(0, 0, (5n * 60n + 45n) * 1_000_000_000n);
            expect(duration.toString()).toBe('PT5M45S');
        });

        it('should format hours, minutes, and seconds', () => {
            const duration = DurationValue.new(0, 0, (2n * 60n * 60n + 30n * 60n + 45n) * 1_000_000_000n);
            expect(duration.toString()).toBe('PT2H30M45S');
        });
    });

    describe('display combined date time', () => {
        it('should format days and hours', () => {
            const duration = DurationValue.new(0, 1, 2n * 60n * 60n * 1_000_000_000n);
            expect(duration.toString()).toBe('P1DT2H');
        });

        it('should format days and minutes', () => {
            const duration = DurationValue.new(0, 1, 30n * 60n * 1_000_000_000n);
            expect(duration.toString()).toBe('P1DT30M');
        });

        it('should format days, hours, and minutes', () => {
            const duration = DurationValue.new(0, 1, (2n * 60n * 60n + 30n * 60n) * 1_000_000_000n);
            expect(duration.toString()).toBe('P1DT2H30M');
        });

        it('should format days, hours, minutes, and seconds', () => {
            const duration = DurationValue.new(0, 1, (2n * 60n * 60n + 30n * 60n + 45n) * 1_000_000_000n);
            expect(duration.toString()).toBe('P1DT2H30M45S');
        });
    });

    describe('display years and months', () => {
        it('should format years and months', () => {
            const duration = DurationValue.new(13, 0, 0n); // 1 year 1 month
            expect(duration.toString()).toBe('P1Y1M');
        });

        it('should format multiple years and months', () => {
            const duration = DurationValue.new(27, 0, 0n); // 2 years 3 months
            expect(duration.toString()).toBe('P2Y3M');
        });
    });

    describe('display milliseconds', () => {
        it('should format milliseconds correctly', () => {
            expect(DurationValue.fromMilliseconds(123).toString()).toBe('PT0.123S');
            expect(DurationValue.fromMilliseconds(1).toString()).toBe('PT0.001S');
            expect(DurationValue.fromMilliseconds(999).toString()).toBe('PT0.999S');
            expect(DurationValue.fromMilliseconds(1500).toString()).toBe('PT1.5S');
        });
    });

    describe('display microseconds', () => {
        it('should format microseconds correctly', () => {
            expect(DurationValue.fromMicroseconds(123456).toString()).toBe('PT0.123456S');
            expect(DurationValue.fromMicroseconds(1).toString()).toBe('PT0.000001S');
            expect(DurationValue.fromMicroseconds(999999).toString()).toBe('PT0.999999S');
            expect(DurationValue.fromMicroseconds(1500000).toString()).toBe('PT1.5S');
        });
    });

    describe('display nanoseconds', () => {
        it('should format nanoseconds correctly', () => {
            expect(DurationValue.fromNanoseconds(123456789n).toString()).toBe('PT0.123456789S');
            expect(DurationValue.fromNanoseconds(1n).toString()).toBe('PT0.000000001S');
            expect(DurationValue.fromNanoseconds(999999999n).toString()).toBe('PT0.999999999S');
            expect(DurationValue.fromNanoseconds(1500000000n).toString()).toBe('PT1.5S');
        });
    });

    describe('display fractional seconds with integers', () => {
        it('should format seconds with milliseconds', () => {
            const duration = DurationValue.new(0, 0, 1n * 1_000_000_000n + 500n * 1_000_000n);
            expect(duration.toString()).toBe('PT1.5S');
        });

        it('should format seconds with microseconds', () => {
            const duration = DurationValue.new(0, 0, 2n * 1_000_000_000n + 123456n * 1_000n);
            expect(duration.toString()).toBe('PT2.123456S');
        });

        it('should format seconds with nanoseconds', () => {
            const duration = DurationValue.new(0, 0, 3n * 1_000_000_000n + 123456789n);
            expect(duration.toString()).toBe('PT3.123456789S');
        });
    });

    describe('display comptokenize durations', () => {
        it('should format comptokenize duration with all components', () => {
            const duration = DurationValue.new(
                0, 
                1, 
                (2n * 60n * 60n + 30n * 60n + 45n) * 1_000_000_000n + 123n * 1_000_000n
            );
            expect(duration.toString()).toBe('P1DT2H30M45.123S');
        });

        it('should format another comptokenize duration', () => {
            const duration = DurationValue.new(
                0,
                7,
                (12n * 60n * 60n + 45n * 60n + 30n) * 1_000_000_000n + 456789n * 1_000n
            );
            expect(duration.toString()).toBe('P7DT12H45M30.456789S');
        });
    });

    describe('display trailing zeros removed', () => {
        it('should remove trailing zeros from fractional seconds', () => {
            expect(DurationValue.fromNanoseconds(100000000n).toString()).toBe('PT0.1S');
            expect(DurationValue.fromNanoseconds(120000000n).toString()).toBe('PT0.12S');
            expect(DurationValue.fromNanoseconds(123000000n).toString()).toBe('PT0.123S');
            expect(DurationValue.fromNanoseconds(123400000n).toString()).toBe('PT0.1234S');
            expect(DurationValue.fromNanoseconds(123450000n).toString()).toBe('PT0.12345S');
            expect(DurationValue.fromNanoseconds(123456000n).toString()).toBe('PT0.123456S');
            expect(DurationValue.fromNanoseconds(123456700n).toString()).toBe('PT0.1234567S');
            expect(DurationValue.fromNanoseconds(123456780n).toString()).toBe('PT0.12345678S');
            expect(DurationValue.fromNanoseconds(123456789n).toString()).toBe('PT0.123456789S');
        });
    });

    describe('display negative durations', () => {
        it('should format negative durations', () => {
            expect(DurationValue.fromSeconds(-30).toString()).toBe('PT-30S');
            expect(DurationValue.fromMinutes(-5).toString()).toBe('PT-5M');
            expect(DurationValue.fromHours(-2).toString()).toBe('PT-2H');
            expect(DurationValue.fromDays(-1).toString()).toBe('P-1D');
        });
    });

    describe('display large values', () => {
        it('should format large durations', () => {
            expect(DurationValue.fromDays(1000).toString()).toBe('P1000D');
            expect(DurationValue.fromHours(25).toString()).toBe('P1DT1H');
            expect(DurationValue.fromMinutes(1500).toString()).toBe('P1DT1H'); // 25 hours
            expect(DurationValue.fromSeconds(90000).toString()).toBe('P1DT1H'); // 25 hours
        });
    });

    describe('display edge cases', () => {
        it('should format edge cases', () => {
            expect(DurationValue.fromNanoseconds(1n).toString()).toBe('PT0.000000001S');
            expect(DurationValue.fromNanoseconds(999999999n).toString()).toBe('PT0.999999999S');
            expect(DurationValue.fromNanoseconds(1000000000n).toString()).toBe('PT1S');
            expect(DurationValue.fromNanoseconds(60n * 1000000000n).toString()).toBe('PT1M');
            expect(DurationValue.fromNanoseconds(3600n * 1000000000n).toString()).toBe('PT1H');
            expect(DurationValue.fromNanoseconds(86400n * 1000000000n).toString()).toBe('P1D');
        });
    });

    describe('display precision boundaries', () => {
        it('should format precision boundaries', () => {
            expect(DurationValue.fromNanoseconds(100n).toString()).toBe('PT0.0000001S');
            expect(DurationValue.fromNanoseconds(10n).toString()).toBe('PT0.00000001S');
            expect(DurationValue.fromNanoseconds(1n).toString()).toBe('PT0.000000001S');
        });
    });

    describe('display from nanos', () => {
        it('should format from nanoseconds', () => {
            expect(DurationValue.fromNanoseconds(123456789n).toString()).toBe('PT0.123456789S');
            expect(DurationValue.fromNanoseconds(3661000000000n).toString()).toBe('PT1H1M1S'); // 1 hour 1 minute 1 second
        });
    });

    describe('abs and negate', () => {
        it('should calculate absolute value', () => {
            const duration = DurationValue.fromSeconds(-30);
            const absDuration = duration.abs();
            expect(absDuration.toString()).toBe('PT30S');
        });

        it('should negate duration', () => {
            const duration = DurationValue.fromSeconds(30);
            const negDuration = duration.negate();
            expect(negDuration.toString()).toBe('PT-30S');
        });
    });

    describe('parse', () => {
        it('should parse valid duration strings', () => {
            expect(DurationValue.parse('PT0S').toString()).toBe('PT0S');
            expect(DurationValue.parse('PT1S').toString()).toBe('PT1S');
            expect(DurationValue.parse('PT1M').toString()).toBe('PT1M');
            expect(DurationValue.parse('PT1H').toString()).toBe('PT1H');
            expect(DurationValue.parse('P1D').toString()).toBe('P1D');
            expect(DurationValue.parse('P1M').toString()).toBe('P1M');
            expect(DurationValue.parse('P1Y').toString()).toBe('P1Y');
        });

        it('should parse comptokenize duration strings', () => {
            expect(DurationValue.parse('P1Y2M3DT4H5M6S').toString()).toBe('P1Y2M3DT4H5M6S');
            expect(DurationValue.parse('PT1H30M').toString()).toBe('PT1H30M');
            expect(DurationValue.parse('P7DT12H').toString()).toBe('P7DT12H');
        });

        it('should parse fractional seconds', () => {
            expect(DurationValue.parse('PT0.123S').toString()).toBe('PT0.123S');
            expect(DurationValue.parse('PT1.5S').toString()).toBe('PT1.5S');
            expect(DurationValue.parse('PT0.123456789S').toString()).toBe('PT0.123456789S');
        });

        it('should parse negative durations', () => {
            expect(DurationValue.parse('-PT30S').toString()).toBe('PT-30S');
            expect(DurationValue.parse('-P1D').toString()).toBe('P-1D');
        });

        it('should return undefined for empty string', () => {
            expect(DurationValue.parse('').value).toBeUndefined();
            expect(DurationValue.parse('   ').value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            expect(DurationValue.parse('⟪undefined⟫').value).toBeUndefined();
        });

        it('should throw error for invalid formats', () => {
            expect(() => DurationValue.parse('invalid')).toThrow('Cannot parse');
            expect(() => DurationValue.parse('30S')).toThrow('Cannot parse'); // Missing P
            expect(() => DurationValue.parse('P')).toThrow('Cannot parse'); // Missing values
        });
    });

    describe('component accessors', () => {
        it('should return correct components', () => {
            const duration = DurationValue.new(13, 7, 123456789n);
            expect(duration.getMonths()).toBe(13);
            expect(duration.getDays()).toBe(7);
            expect(duration.getNanos()).toBe(123456789n);
        });

        it('should return undefined for undefined duration', () => {
            const duration = new DurationValue(undefined);
            expect(duration.getMonths()).toBeUndefined();
            expect(duration.getDays()).toBeUndefined();
            expect(duration.getNanos()).toBeUndefined();
        });
    });

    describe('conversion methods', () => {
        it('should convert to seconds', () => {
            const duration = DurationValue.fromSeconds(30);
            expect(duration.seconds()).toBe(30n);
        });

        it('should convert to milliseconds', () => {
            const duration = DurationValue.fromMilliseconds(1500);
            expect(duration.milliseconds()).toBe(1500n);
        });

        it('should convert to microseconds', () => {
            const duration = DurationValue.fromMicroseconds(1500000);
            expect(duration.microseconds()).toBe(1500000n);
        });

        it('should convert to nanoseconds', () => {
            const duration = DurationValue.fromNanoseconds(1500000000n);
            expect(duration.nanoseconds()).toBe(1500000000n);
        });
    });

    describe('isPositive and isNegative', () => {
        it('should detect positive durations', () => {
            expect(DurationValue.fromSeconds(1).isPositive()).toBe(true);
            expect(DurationValue.fromDays(1).isPositive()).toBe(true);
            expect(DurationValue.fromMonths(1).isPositive()).toBe(true);
        });

        it('should detect negative durations', () => {
            expect(DurationValue.fromSeconds(-1).isNegative()).toBe(true);
            expect(DurationValue.fromDays(-1).isNegative()).toBe(true);
            expect(DurationValue.fromMonths(-1).isNegative()).toBe(true);
        });

        it('should handle zero duration', () => {
            const zero = DurationValue.zero();
            expect(zero.isPositive()).toBe(false);
            expect(zero.isNegative()).toBe(false);
        });

        it('should handle undefined duration', () => {
            const undef = new DurationValue(undefined);
            expect(undef.isPositive()).toBe(false);
            expect(undef.isNegative()).toBe(false);
        });
    });

    describe('valueOf', () => {
        it('should return the internal value', () => {
            const duration = DurationValue.new(1, 2, 3n);
            const value = duration.valueOf();
            expect(value).toEqual({ months: 1, days: 2, nanos: 3n });
        });

        it('should return undefined when value is undefined', () => {
            const duration = new DurationValue(undefined);
            expect(duration.valueOf()).toBeUndefined();
        });
    });
});