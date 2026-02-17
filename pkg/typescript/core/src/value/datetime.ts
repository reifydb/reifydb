// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {Type, Value, TypeValuePair} from ".";
import {NONE_VALUE} from "../constant";
import {DateValue} from "./date";
import {TimeValue} from "./time";

/**
 * A date and time value with nanosecond precision.
 * Always in UTC timezone.
 * Internally stored as months, days, and nanoseconds.
 */
export class DateTimeValue implements Value {
    readonly type: Type = "DateTime" as const;
    private readonly months?: number;  // years*12 + months  
    private readonly days?: number;    // day of month (1-31)
    private readonly nanos?: bigint;   // nanoseconds since midnight

    constructor(value?: Date | string | number | bigint) {
        if (value !== undefined) {
            if (value instanceof Date) {
                // Store as UTC with millisecond precision from Date
                const year = value.getUTCFullYear();
                const month = value.getUTCMonth() + 1;
                const day = value.getUTCDate();
                const hour = value.getUTCHours();
                const minute = value.getUTCMinutes();
                const second = value.getUTCSeconds();
                const millis = value.getUTCMilliseconds();

                this.months = year * 12 + (month - 1);
                this.days = day;
                this.nanos = BigInt(hour) * 3_600_000_000_000n +
                    BigInt(minute) * 60_000_000_000n +
                    BigInt(second) * 1_000_000_000n +
                    BigInt(millis) * 1_000_000n;
            } else if (typeof value === 'string') {
                // Parse ISO 8601 format
                const parsed = DateTimeValue.parseDateTime(value);
                if (!parsed) {
                    throw new Error(`Invalid datetime string: ${value}`);
                }
                this.months = parsed.months;
                this.days = parsed.days;
                this.nanos = parsed.nanos;
            } else if (typeof value === 'number') {
                // Interpret as milliseconds since epoch
                const date = new Date(value);
                const year = date.getUTCFullYear();
                const month = date.getUTCMonth() + 1;
                const day = date.getUTCDate();
                const hour = date.getUTCHours();
                const minute = date.getUTCMinutes();
                const second = date.getUTCSeconds();
                const millis = date.getUTCMilliseconds();

                this.months = year * 12 + (month - 1);
                this.days = day;
                this.nanos = BigInt(hour) * 3_600_000_000_000n +
                    BigInt(minute) * 60_000_000_000n +
                    BigInt(second) * 1_000_000_000n +
                    BigInt(millis) * 1_000_000n;
            } else if (typeof value === 'bigint') {
                // Interpret as nanoseconds since epoch
                const millis = Number(value / 1_000_000n);
                const extraNanos = value % 1_000_000n;
                const date = new Date(millis);

                const year = date.getUTCFullYear();
                const month = date.getUTCMonth() + 1;
                const day = date.getUTCDate();
                const hour = date.getUTCHours();
                const minute = date.getUTCMinutes();
                const second = date.getUTCSeconds();
                const dateMillis = date.getUTCMilliseconds();

                this.months = year * 12 + (month - 1);
                this.days = day;
                this.nanos = BigInt(hour) * 3_600_000_000_000n +
                    BigInt(minute) * 60_000_000_000n +
                    BigInt(second) * 1_000_000_000n +
                    BigInt(dateMillis) * 1_000_000n +
                    extraNanos;
            } else {
                throw new Error(`DateTime value must be a Date, string, number, or bigint, got ${typeof value}`);
            }
        } else {
            this.months = undefined;
            this.days = undefined;
            this.nanos = undefined;
        }
    }

    /**
     * Create a DateTimeValue from year, month, day, hour, minute, second, nanosecond
     */
    static fromYMDHMSN(
        year: number,
        month: number,
        day: number,
        hour: number,
        minute: number,
        second: number,
        nano: number = 0
    ): DateTimeValue {
        // Validate inputs
        if (hour < 0 || hour > 23) {
            throw new Error(`Invalid hour: ${hour}`);
        }
        if (minute < 0 || minute > 59) {
            throw new Error(`Invalid minute: ${minute}`);
        }
        if (second < 0 || second > 59) {
            throw new Error(`Invalid second: ${second}`);
        }
        if (nano < 0 || nano > 999_999_999) {
            throw new Error(`Invalid nanosecond: ${nano}`);
        }

        // Validate the date
        if (!DateTimeValue.isValidDate(year, month, day)) {
            throw new Error(`Invalid datetime: ${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')} ${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:${String(second).padStart(2, '0')}`);
        }

        const result = new DateTimeValue(undefined);
        (result as any).months = year * 12 + (month - 1);
        (result as any).days = day;
        (result as any).nanos = BigInt(hour) * 3_600_000_000_000n +
            BigInt(minute) * 60_000_000_000n +
            BigInt(second) * 1_000_000_000n +
            BigInt(nano);
        return result;
    }

    /**
     * Create a DateTimeValue from year, month, day, hour, minute, second (no fractional)
     */
    static fromYMDHMS(
        year: number,
        month: number,
        day: number,
        hour: number,
        minute: number,
        second: number
    ): DateTimeValue {
        return DateTimeValue.fromYMDHMSN(year, month, day, hour, minute, second, 0);
    }

    /**
     * Create from Unix timestamp in seconds
     */
    static fromTimestamp(seconds: number): DateTimeValue {
        return new DateTimeValue(seconds * 1000);
    }

    /**
     * Create from Unix timestamp in milliseconds
     */
    static fromTimestampMillis(millis: number): DateTimeValue {
        return new DateTimeValue(millis);
    }

    /**
     * Create from nanoseconds since Unix epoch
     */
    static fromNanosSinceEpoch(nanos: bigint): DateTimeValue {
        return new DateTimeValue(nanos);
    }

    /**
     * Create from separate seconds and nanoseconds
     */
    static fromParts(seconds: number, nanos: number): DateTimeValue {
        if (nanos < 0 || nanos > 999_999_999) {
            throw new Error(`Invalid nanoseconds: ${nanos}`);
        }
        const millis = seconds * 1000 + Math.floor(nanos / 1_000_000);
        const extraNanos = nanos % 1_000_000;
        const result = new DateTimeValue(millis);

        // Add the extra nanoseconds
        if (result.nanos !== undefined) {
            (result as any).nanos = result.nanos + BigInt(extraNanos);
        }
        return result;
    }

    /**
     * Get current datetime
     */
    static now(): DateTimeValue {
        return new DateTimeValue(new Date());
    }

    /**
     * Get default datetime (Unix epoch)
     */
    static default(): DateTimeValue {
        return DateTimeValue.fromYMDHMS(1970, 1, 1, 0, 0, 0);
    }

    /**
     * Parse a datetime string in ISO 8601 format
     */
    static parse(str: string): DateTimeValue {
        const trimmed = str.trim();

        if (trimmed === '' || trimmed === NONE_VALUE) {
            return new DateTimeValue(undefined);
        }

        const parsed = DateTimeValue.parseDateTime(trimmed);
        if (!parsed) {
            throw new Error(`Cannot parse "${str}" as DateTime`);
        }

        const result = new DateTimeValue(undefined);
        (result as any).months = parsed.months;
        (result as any).days = parsed.days;
        (result as any).nanos = parsed.nanos;
        return result;
    }

    /**
     * Get Unix timestamp in seconds
     */
    timestamp(): number | undefined {
        const date = this.valueOf();
        if (date === undefined) return undefined;
        return Math.floor(date.getTime() / 1000);
    }

    /**
     * Get Unix timestamp in nanoseconds
     */
    timestampNanos(): bigint | undefined {
        const date = this.valueOf();
        if (date === undefined || this.nanos === undefined) return undefined;

        // Convert date to epoch nanoseconds
        const epochNanos = BigInt(date.getTime()) * 1_000_000n;

        // The nanos field stores time-of-day, but we already included millis in epochNanos
        // So we need to add only the sub-millisecond portion
        const subMillisNanos = this.nanos % 1_000_000n;

        return epochNanos + subMillisNanos;
    }

    /**
     * Convert to nanoseconds since Unix epoch for storage
     */
    toNanosSinceEpoch(): bigint | undefined {
        return this.timestampNanos();
    }

    /**
     * Get separate seconds and nanoseconds for storage
     */
    toParts(): [number, number] | undefined {
        const date = this.valueOf();
        if (date === undefined || this.nanos === undefined) return undefined;

        const seconds = Math.floor(date.getTime() / 1000);
        const millis = date.getTime() % 1000;
        const subMillisNanos = Number(this.nanos % 1_000_000n);
        const nanos = millis * 1_000_000 + subMillisNanos;
        return [seconds, nanos];
    }

    /**
     * Get the date component
     */
    date(): DateValue | undefined {
        if (this.months === undefined || this.days === undefined) return undefined;
        const year = Math.floor(this.months / 12);
        // Handle negative months correctly
        let m = this.months % 12;
        if (m < 0) m += 12;
        const month = m + 1;
        return DateValue.fromYMD(year, month, this.days);
    }

    /**
     * Get the time component
     */
    time(): TimeValue | undefined {
        if (this.nanos === undefined) return undefined;
        return new TimeValue(this.nanos);
    }

    /**
     * Format as ISO 8601 string with nanosecond precision and Z suffix
     */
    toString(): string {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return 'none';
        }

        const year = Math.floor(this.months / 12);
        // Handle negative months correctly
        let m = this.months % 12;
        if (m < 0) m += 12;
        const month = String(m + 1).padStart(2, '0');
        const day = String(this.days).padStart(2, '0');

        // Extract time components from nanos
        const totalNanos = this.nanos;
        const hours = totalNanos / 3_600_000_000_000n;
        const remainingAfterHours = totalNanos % 3_600_000_000_000n;
        const minutes = remainingAfterHours / 60_000_000_000n;
        const remainingAfterMinutes = remainingAfterHours % 60_000_000_000n;
        const seconds = remainingAfterMinutes / 1_000_000_000n;
        const nanosFraction = remainingAfterMinutes % 1_000_000_000n;

        const hour = String(Number(hours)).padStart(2, '0');
        const minute = String(Number(minutes)).padStart(2, '0');
        const second = String(Number(seconds)).padStart(2, '0');
        const nanoStr = String(Number(nanosFraction)).padStart(9, '0');

        // Handle negative years (BC dates)
        let yearStr: string;
        if (year < 0) {
            const absYear = Math.abs(year);
            yearStr = `-${String(absYear).padStart(4, '0')}`;
        } else {
            yearStr = String(year).padStart(4, '0');
        }

        return `${yearStr}-${month}-${day}T${hour}:${minute}:${second}.${nanoStr}Z`;
    }

    valueOf(): Date | undefined {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return undefined;
        }

        const year = Math.floor(this.months / 12);
        // Handle negative months correctly
        let m = this.months % 12;
        if (m < 0) m += 12;
        const month = m + 1;

        // Extract time components
        const totalNanos = this.nanos;
        const hours = Number(totalNanos / 3_600_000_000_000n);
        const remainingAfterHours = totalNanos % 3_600_000_000_000n;
        const minutes = Number(remainingAfterHours / 60_000_000_000n);
        const remainingAfterMinutes = remainingAfterHours % 60_000_000_000n;
        const seconds = Number(remainingAfterMinutes / 1_000_000_000n);
        const nanosFraction = remainingAfterMinutes % 1_000_000_000n;
        const millis = Number(nanosFraction / 1_000_000n);

        // Handle years < 100 specially
        let date: Date;
        if (year >= 0 && year < 100) {
            date = new Date(Date.UTC(2000, month - 1, this.days, hours, minutes, seconds, millis));
            date.setUTCFullYear(year);
        } else {
            date = new Date(Date.UTC(year, month - 1, this.days, hours, minutes, seconds, millis));
        }

        return date;
    }

    /**
     * Get the internal representation as a Date
     */
    get value(): Date | undefined {
        return this.valueOf();
    }

    /**
     * Helper to parse ISO 8601 datetime format
     */
    private static parseDateTime(str: string): { months: number; days: number; nanos: bigint } | null {
        // Match ISO 8601 format: YYYY-MM-DDTHH:MM:SS[.nnnnnnnnn]Z
        const match = str.match(/^(-?\d{1,4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/);
        if (!match) {
            return null;
        }

        const year = parseInt(match[1], 10);
        const month = parseInt(match[2], 10);
        const day = parseInt(match[3], 10);
        const hour = parseInt(match[4], 10);
        const minute = parseInt(match[5], 10);
        const second = parseInt(match[6], 10);

        // Parse fractional seconds if present
        let nanosFraction = 0n;
        if (match[7]) {
            // Pad or truncate to 9 digits
            const fracStr = match[7].padEnd(9, '0').substring(0, 9);
            nanosFraction = BigInt(fracStr);
        }

        // Validate ranges
        if (month < 1 || month > 12) {
            return null;
        }
        if (day < 1 || day > 31) {
            return null;
        }
        if (hour < 0 || hour > 23) {
            return null;
        }
        if (minute < 0 || minute > 59) {
            return null;
        }
        if (second < 0 || second > 59) {
            return null;
        }

        // Validate the date is valid
        if (!DateTimeValue.isValidDate(year, month, day)) {
            return null;
        }

        const months = year * 12 + (month - 1);
        const nanos = BigInt(hour) * 3_600_000_000_000n +
            BigInt(minute) * 60_000_000_000n +
            BigInt(second) * 1_000_000_000n +
            nanosFraction;

        return {months, days: day, nanos};
    }

    /**
     * Helper to validate a date
     */
    private static isValidDate(year: number, month: number, day: number): boolean {
        // Create a date and check if components match
        let date: Date;
        if (year >= 0 && year < 100) {
            date = new Date(Date.UTC(2000, month - 1, day));
            date.setUTCFullYear(year);
        } else {
            date = new Date(Date.UTC(year, month - 1, day));
        }

        return date.getUTCFullYear() === year &&
            date.getUTCMonth() === month - 1 &&
            date.getUTCDate() === day;
    }

    /**
     * Compare two datetimes for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherDateTime = other as DateTimeValue;
        if (this.months === undefined || otherDateTime.months === undefined) {
            return this.months === otherDateTime.months && 
                   this.days === otherDateTime.days && 
                   this.nanos === otherDateTime.nanos;
        }
        
        return this.months === otherDateTime.months && 
               this.days === otherDateTime.days && 
               this.nanos === otherDateTime.nanos;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? NONE_VALUE : this.toString()
        };
    }
}