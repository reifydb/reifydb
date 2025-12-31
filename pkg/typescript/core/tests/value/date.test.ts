// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { describe, expect, it } from 'vitest';
import { DateValue } from '../../src';

describe('DateValue', () => {
    describe('constructor', () => {
        it('should create instance with Date object', () => {
            const jsDate = new Date(Date.UTC(2024, 2, 15)); // March 15, 2024
            const date = new DateValue(jsDate);
            expect(date.value).toBeDefined();
            expect(date.type).toBe('Date');
            expect(date.toString()).toBe('2024-03-15');
        });

        it('should create instance with date string', () => {
            const date = new DateValue('2024-03-15');
            expect(date.toString()).toBe('2024-03-15');
        });

        it('should create instance with days since epoch', () => {
            const date = new DateValue(0); // Unix epoch
            expect(date.toString()).toBe('1970-01-01');
        });

        it('should create instance with undefined', () => {
            const date = new DateValue(undefined);
            expect(date.value).toBeUndefined();
            expect(date.toString()).toBe('undefined');
        });

        it('should remove time component from Date', () => {
            const jsDate = new Date('2024-03-15T15:30:45.123Z');
            const date = new DateValue(jsDate);
            expect(date.value?.getUTCHours()).toBe(0);
            expect(date.value?.getUTCMinutes()).toBe(0);
            expect(date.value?.getUTCSeconds()).toBe(0);
            expect(date.value?.getUTCMilliseconds()).toBe(0);
        });

        it('should throw error for invalid date string', () => {
            expect(() => new DateValue('invalid')).toThrow('Invalid date string: invalid');
            expect(() => new DateValue('2024-13-01')).toThrow('Invalid date string: 2024-13-01');
            expect(() => new DateValue('2024-02-30')).toThrow('Invalid date string: 2024-02-30');
        });
    });

    describe('fromYMD', () => {
        it('should create date from year, month, day', () => {
            const date = DateValue.fromYMD(2024, 3, 15);
            expect(date.toString()).toBe('2024-03-15');
            expect(date.year()).toBe(2024);
            expect(date.month()).toBe(3);
            expect(date.day()).toBe(15);
        });

        it('should handle leap year', () => {
            const date = DateValue.fromYMD(2024, 2, 29);
            expect(date.toString()).toBe('2024-02-29');
        });

        it('should throw error for invalid dates', () => {
            expect(() => DateValue.fromYMD(2024, 2, 30)).toThrow('Invalid date: 2024-02-30');
            expect(() => DateValue.fromYMD(2024, 13, 1)).toThrow('Invalid date: 2024-13-01');
            expect(() => DateValue.fromYMD(2024, 0, 15)).toThrow('Invalid date: 2024-00-15');
            expect(() => DateValue.fromYMD(2023, 2, 29)).toThrow('Invalid date: 2023-02-29'); // Not a leap year
        });
    });

    describe('display standard dates', () => {
        it('should format standard dates correctly', () => {
            expect(DateValue.fromYMD(2024, 3, 15).toString()).toBe('2024-03-15');
            expect(DateValue.fromYMD(2000, 1, 1).toString()).toBe('2000-01-01');
            expect(DateValue.fromYMD(1999, 12, 31).toString()).toBe('1999-12-31');
        });
    });

    describe('display edge cases', () => {
        it('should format Unix epoch', () => {
            expect(DateValue.fromYMD(1970, 1, 1).toString()).toBe('1970-01-01');
        });

        it('should format leap year date', () => {
            expect(DateValue.fromYMD(2024, 2, 29).toString()).toBe('2024-02-29');
        });

        it('should format single digit day/month with padding', () => {
            expect(DateValue.fromYMD(2024, 1, 9).toString()).toBe('2024-01-09');
            expect(DateValue.fromYMD(2024, 9, 1).toString()).toBe('2024-09-01');
        });
    });

    describe('display boundary dates', () => {
        it('should format very early date', () => {
            expect(DateValue.fromYMD(1, 1, 1).toString()).toBe('0001-01-01');
        });

        it('should format far future date', () => {
            expect(DateValue.fromYMD(9999, 12, 31).toString()).toBe('9999-12-31');
        });

        it('should format century boundaries', () => {
            expect(DateValue.fromYMD(1900, 1, 1).toString()).toBe('1900-01-01');
            expect(DateValue.fromYMD(2000, 1, 1).toString()).toBe('2000-01-01');
            expect(DateValue.fromYMD(2100, 1, 1).toString()).toBe('2100-01-01');
        });
    });

    describe('display negative years', () => {
        it('should format year 0 (1 BC)', () => {
            expect(DateValue.fromYMD(0, 1, 1).toString()).toBe('0000-01-01');
        });

        it('should format negative years (BC)', () => {
            expect(DateValue.fromYMD(-1, 1, 1).toString()).toBe('-0001-01-01');
            expect(DateValue.fromYMD(-100, 12, 31).toString()).toBe('-0100-12-31');
        });
    });

    describe('display all months', () => {
        const months: [number, string][] = [
            [1, '01'], [2, '02'], [3, '03'], [4, '04'],
            [5, '05'], [6, '06'], [7, '07'], [8, '08'],
            [9, '09'], [10, '10'], [11, '11'], [12, '12']
        ];

        months.forEach(([month, expected]) => {
            it(`should format month ${month} as ${expected}`, () => {
                expect(DateValue.fromYMD(2024, month, 15).toString()).toBe(`2024-${expected}-15`);
            });
        });
    });

    describe('display days in month', () => {
        const testCases: [number, number, number, string][] = [
            [2024, 1, 1, '2024-01-01'],
            [2024, 1, 31, '2024-01-31'],
            [2024, 2, 1, '2024-02-01'],
            [2024, 2, 29, '2024-02-29'], // Leap year
            [2024, 4, 1, '2024-04-01'],
            [2024, 4, 30, '2024-04-30'],
            [2024, 12, 1, '2024-12-01'],
            [2024, 12, 31, '2024-12-31']
        ];

        testCases.forEach(([year, month, day, expected]) => {
            it(`should format ${year}-${month}-${day} as ${expected}`, () => {
                expect(DateValue.fromYMD(year, month, day).toString()).toBe(expected);
            });
        });
    });

    describe('parse', () => {
        it('should parse valid date strings', () => {
            expect(DateValue.parse('2024-03-15').toString()).toBe('2024-03-15');
            expect(DateValue.parse('1970-01-01').toString()).toBe('1970-01-01');
            expect(DateValue.parse('0001-01-01').toString()).toBe('0001-01-01');
            expect(DateValue.parse('-0001-01-01').toString()).toBe('-0001-01-01');
        });

        it('should parse with whitespace', () => {
            expect(DateValue.parse('  2024-03-15  ').toString()).toBe('2024-03-15');
        });

        it('should return undefined for empty string', () => {
            expect(DateValue.parse('').value).toBeUndefined();
            expect(DateValue.parse('   ').value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            expect(DateValue.parse('⟪undefined⟫').value).toBeUndefined();
        });

        it('should throw error for invalid formats', () => {
            expect(() => DateValue.parse('2024/03/15')).toThrow('Cannot parse "2024/03/15" as Date');
            expect(() => DateValue.parse('03-15-2024')).toThrow('Cannot parse "03-15-2024" as Date');
            expect(() => DateValue.parse('March 15, 2024')).toThrow('Cannot parse "March 15, 2024" as Date');
            expect(() => DateValue.parse('2024-3-15')).toThrow('Cannot parse "2024-3-15" as Date'); // Requires padding
        });

        it('should throw error for invalid dates', () => {
            expect(() => DateValue.parse('2024-02-30')).toThrow('Cannot parse "2024-02-30" as Date');
            expect(() => DateValue.parse('2024-13-01')).toThrow('Cannot parse "2024-13-01" as Date');
            expect(() => DateValue.parse('2024-00-15')).toThrow('Cannot parse "2024-00-15" as Date');
            expect(() => DateValue.parse('2024-01-32')).toThrow('Cannot parse "2024-01-32" as Date');
        });
    });

    describe('toDaysSinceEpoch and fromDaysSinceEpoch', () => {
        it('should convert to and from days since epoch', () => {
            const date = DateValue.fromYMD(1970, 1, 1);
            expect(date.toDaysSinceEpoch()).toBe(0);

            const date2 = DateValue.fromYMD(1970, 1, 2);
            expect(date2.toDaysSinceEpoch()).toBe(1);

            const date3 = DateValue.fromYMD(2024, 3, 15);
            const days = date3.toDaysSinceEpoch()!;
            const reconstructed = new DateValue(days);
            expect(reconstructed.toString()).toBe('2024-03-15');
        });

        it('should handle negative days for dates before epoch', () => {
            const date = DateValue.fromYMD(1969, 12, 31);
            expect(date.toDaysSinceEpoch()).toBe(-1);

            const date2 = DateValue.fromYMD(1969, 12, 30);
            expect(date2.toDaysSinceEpoch()).toBe(-2);
        });

        it('should return undefined for undefined value', () => {
            const date = new DateValue(undefined);
            expect(date.toDaysSinceEpoch()).toBeUndefined();
        });

        it('should round-trip correctly', () => {
            const testDates = [
                '1970-01-01',
                '2024-03-15',
                '1969-12-31',
                '2000-02-29',
                '0001-01-01',
                '9999-12-31'
            ];

            testDates.forEach(dateStr => {
                const original = DateValue.parse(dateStr);
                const days = original.toDaysSinceEpoch()!;
                const reconstructed = new DateValue(days);
                expect(reconstructed.toString()).toBe(dateStr);
            });
        });
    });

    describe('today', () => {
        it('should create today\'s date', () => {
            const today = DateValue.today();
            const now = new Date();
            
            expect(today.year()).toBe(now.getUTCFullYear());
            expect(today.month()).toBe(now.getUTCMonth() + 1);
            expect(today.day()).toBe(now.getUTCDate());
            
            // Should have no time component
            expect(today.value?.getUTCHours()).toBe(0);
            expect(today.value?.getUTCMinutes()).toBe(0);
            expect(today.value?.getUTCSeconds()).toBe(0);
        });
    });

    describe('component accessors', () => {
        it('should return correct year, month, day', () => {
            const date = DateValue.fromYMD(2024, 3, 15);
            expect(date.year()).toBe(2024);
            expect(date.month()).toBe(3);
            expect(date.day()).toBe(15);
        });

        it('should return undefined for undefined date', () => {
            const date = new DateValue(undefined);
            expect(date.year()).toBeUndefined();
            expect(date.month()).toBeUndefined();
            expect(date.day()).toBeUndefined();
        });
    });

    describe('valueOf', () => {
        it('should return the Date value', () => {
            const jsDate = new Date(Date.UTC(2024, 2, 15));
            const date = new DateValue(jsDate);
            expect(date.valueOf()).toBeInstanceOf(Date);
            expect(date.valueOf()?.getUTCFullYear()).toBe(2024);
        });

        it('should return undefined when value is undefined', () => {
            const date = new DateValue(undefined);
            expect(date.valueOf()).toBeUndefined();
        });
    });
});