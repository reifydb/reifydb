/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Interval} from '../../src/interval';

describe('Interval', () => {
    describe('parse', () => {
        it('should parse simple intervals', () => {
            const interval = Interval.parse('P1D');
            expect(interval.totalNanoseconds).toBe(BigInt(24 * 60 * 60 * 1_000_000_000));
            expect(interval.toString()).toBe('P1D');
        });

        it('should parse complex intervals', () => {
            const interval = Interval.parse('P1Y2M3DT4H5M6S');
            const expected = BigInt(
                365 * 24 * 60 * 60 * 1_000_000_000 +     // 1 year
                2 * 30 * 24 * 60 * 60 * 1_000_000_000 +  // 2 months
                3 * 24 * 60 * 60 * 1_000_000_000 +       // 3 days
                4 * 60 * 60 * 1_000_000_000 +            // 4 hours
                5 * 60 * 1_000_000_000 +                 // 5 minutes
                6 * 1_000_000_000                        // 6 seconds
            );
            expect(interval.totalNanoseconds).toBe(expected);
        });

        it('should throw error for invalid formats', () => {
            expect(() => Interval.parse('invalid')).toThrow('Invalid interval format - must start with P');
            expect(() => Interval.parse('P1X')).toThrow('Invalid character in interval: X');
        });
    });

    describe('from', () => {
        it('should create interval from components', () => {
            const interval = Interval.from({
                days: 1,
                hours: 2,
                minutes: 30,
                seconds: 45
            });

            const expected = BigInt(
                1 * 24 * 60 * 60 * 1_000_000_000 +  // 1 day
                2 * 60 * 60 * 1_000_000_000 +       // 2 hours
                30 * 60 * 1_000_000_000 +           // 30 minutes
                45 * 1_000_000_000                  // 45 seconds
            );
            expect(interval.totalNanoseconds).toBe(expected);
        });

        it('should handle fractional components', () => {
            const interval = Interval.from({
                milliseconds: 123,
                microseconds: 456,
                nanoseconds: 789
            });

            const expected = BigInt(
                123 * 1_000_000 +  // 123 milliseconds
                456 * 1_000 +      // 456 microseconds
                789                // 789 nanoseconds
            );
            expect(interval.totalNanoseconds).toBe(expected);
        });
    });

    describe('components', () => {
        it('should return correct components', () => {
            const interval = Interval.from({
                days: 1,
                hours: 2,
                minutes: 30,
                seconds: 45,
                nanoseconds: 123456789
            });

            const components = interval.components;
            expect(components.days).toBe(BigInt(1));
            expect(components.hours).toBe(BigInt(2));
            expect(components.minutes).toBe(BigInt(30));
            expect(components.seconds).toBe(BigInt(45));
            expect(components.nanoseconds).toBe(BigInt(123456789));
        });
    });

    describe('total getters', () => {
        it('should return correct totals', () => {
            const interval = Interval.from({
                days: 1,
                hours: 2,
                minutes: 30
            });

            expect(interval.totalDays).toBe(BigInt(1));
            expect(interval.totalHours).toBe(BigInt(26)); // 1 day + 2 hours
            expect(interval.totalMinutes).toBe(BigInt(1590)); // 26 hours + 30 minutes
            expect(interval.totalSeconds).toBe(BigInt(95400)); // 1590 minutes
        });
    });

    describe('arithmetic operations', () => {
        it('should add intervals correctly', () => {
            const interval1 = Interval.from({hours: 1});
            const interval2 = Interval.from({minutes: 30});
            const result = interval1.add(interval2);

            expect(result.totalMinutes).toBe(BigInt(90)); // 60 + 30
        });

        it('should subtract intervals correctly', () => {
            const interval1 = Interval.from({hours: 2});
            const interval2 = Interval.from({minutes: 30});
            const result = interval1.subtract(interval2);

            expect(result.totalMinutes).toBe(BigInt(90)); // 120 - 30
        });
    });

    describe('comparisons', () => {
        it('should compare intervals correctly', () => {
            const interval1 = Interval.from({hours: 1});
            const interval2 = Interval.from({minutes: 60});
            const interval3 = Interval.from({hours: 2});

            expect(interval1.equals(interval2)).toBe(true);
            expect(interval1.lessThan(interval3)).toBe(true);
            expect(interval3.greaterThan(interval1)).toBe(true);
        });
    });

    describe('toString', () => {
        it('should convert to ISO 8601 format', () => {
            const interval = Interval.from({
                days: 1,
                hours: 2,
                minutes: 30,
                seconds: 45
            });

            expect(interval.toString()).toBe('P1DT2H30M45S');
        });

        it('should handle zero interval', () => {
            const interval = new Interval(BigInt(0));
            expect(interval.toString()).toBe('PT0S');
        });
    });

    describe('toJSON', () => {
        it('should serialize to JSON', () => {
            const interval = Interval.from({days: 1, hours: 2});
            const json = interval.toJSON();

            expect(json).toHaveProperty('totalNanoseconds');
            expect(json).toHaveProperty('iso8601');
            expect(json.iso8601).toBe('P1DT2H');
        });
    });
});