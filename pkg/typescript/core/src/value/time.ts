/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { Type, Value, TypeValuePair } from "./type";
import { UNDEFINED_VALUE } from "../constant";

/**
 * A time value representing time of day (hour, minute, second, nanosecond) without date information.
 * Internally stored as nanoseconds since midnight.
 */
export class TimeValue implements Value {
    readonly type: Type = "Time" as const;
    public readonly value?: bigint; // nanoseconds since midnight

    private static readonly NANOS_PER_SECOND = 1_000_000_000n;
    private static readonly NANOS_PER_MINUTE = 60_000_000_000n;
    private static readonly NANOS_PER_HOUR = 3_600_000_000_000n;
    private static readonly NANOS_PER_DAY = 86_400_000_000_000n;

    constructor(value?: bigint | string | number) {
        if (value !== undefined) {
            if (typeof value === 'bigint') {
                // Validate range (0 to 24 hours in nanoseconds)
                if (value < 0n || value >= TimeValue.NANOS_PER_DAY) {
                    throw new Error(`Time value must be between 0 and ${TimeValue.NANOS_PER_DAY - 1n} nanoseconds`);
                }
                this.value = value;
            } else if (typeof value === 'string') {
                // Parse HH:MM:SS[.nnnnnnnnn] format
                const parsed = TimeValue.parseTime(value);
                if (parsed === null) {
                    throw new Error(`Invalid time string: ${value}`);
                }
                this.value = parsed;
            } else if (typeof value === 'number') {
                // Accept number as nanoseconds since midnight
                const bigintValue = BigInt(Math.floor(value));
                if (bigintValue < 0n || bigintValue >= TimeValue.NANOS_PER_DAY) {
                    throw new Error(`Time value must be between 0 and ${TimeValue.NANOS_PER_DAY - 1n} nanoseconds`);
                }
                this.value = bigintValue;
            } else {
                throw new Error(`Time value must be a bigint, string, or number, got ${typeof value}`);
            }
        } else {
            this.value = undefined;
        }
    }

    /**
     * Create a TimeValue from hour, minute, second, and nanosecond
     */
    static fromHMSN(hour: number, minute: number, second: number, nano: number = 0): TimeValue {
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

        const nanos = BigInt(hour) * TimeValue.NANOS_PER_HOUR +
                     BigInt(minute) * TimeValue.NANOS_PER_MINUTE +
                     BigInt(second) * TimeValue.NANOS_PER_SECOND +
                     BigInt(nano);

        return new TimeValue(nanos);
    }

    /**
     * Create a TimeValue from hour, minute, and second (no fractional seconds)
     */
    static fromHMS(hour: number, minute: number, second: number): TimeValue {
        return TimeValue.fromHMSN(hour, minute, second, 0);
    }

    /**
     * Create midnight (00:00:00.000000000)
     */
    static midnight(): TimeValue {
        return new TimeValue(0n);
    }

    /**
     * Create noon (12:00:00.000000000)
     */
    static noon(): TimeValue {
        return new TimeValue(12n * TimeValue.NANOS_PER_HOUR);
    }

    /**
     * Parse a time string in HH:MM:SS[.nnnnnnnnn] format
     */
    static parse(str: string): TimeValue {
        const trimmed = str.trim();
        
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new TimeValue(undefined);
        }

        const parsed = TimeValue.parseTime(trimmed);
        if (parsed === null) {
            throw new Error(`Cannot parse "${str}" as Time`);
        }

        return new TimeValue(parsed);
    }

    /**
     * Get the hour component (0-23)
     */
    hour(): number | undefined {
        if (this.value === undefined) return undefined;
        return Number(this.value / TimeValue.NANOS_PER_HOUR);
    }

    /**
     * Get the minute component (0-59)
     */
    minute(): number | undefined {
        if (this.value === undefined) return undefined;
        const remainingAfterHours = this.value % TimeValue.NANOS_PER_HOUR;
        return Number(remainingAfterHours / TimeValue.NANOS_PER_MINUTE);
    }

    /**
     * Get the second component (0-59)
     */
    second(): number | undefined {
        if (this.value === undefined) return undefined;
        const remainingAfterHours = this.value % TimeValue.NANOS_PER_HOUR;
        const remainingAfterMinutes = remainingAfterHours % TimeValue.NANOS_PER_MINUTE;
        return Number(remainingAfterMinutes / TimeValue.NANOS_PER_SECOND);
    }

    /**
     * Get the nanosecond component (0-999999999)
     */
    nanosecond(): number | undefined {
        if (this.value === undefined) return undefined;
        return Number(this.value % TimeValue.NANOS_PER_SECOND);
    }

    /**
     * Convert to nanoseconds since midnight for storage
     */
    toNanosSinceMidnight(): bigint | undefined {
        return this.value;
    }

    /**
     * Create from nanoseconds since midnight
     */
    static fromNanosSinceMidnight(nanos: bigint | number): TimeValue {
        return new TimeValue(typeof nanos === 'number' ? BigInt(nanos) : nanos);
    }

    /**
     * Format as HH:MM:SS.nnnnnnnnn string
     */
    toString(): string {
        if (this.value === undefined) {
            return 'undefined';
        }

        const hour = this.hour()!;
        const minute = this.minute()!;
        const second = this.second()!;
        const nano = this.nanosecond()!;

        const hourStr = String(hour).padStart(2, '0');
        const minuteStr = String(minute).padStart(2, '0');
        const secondStr = String(second).padStart(2, '0');
        const nanoStr = String(nano).padStart(9, '0');

        return `${hourStr}:${minuteStr}:${secondStr}.${nanoStr}`;
    }

    valueOf(): bigint | undefined {
        return this.value;
    }

    /**
     * Helper to parse HH:MM:SS[.nnnnnnnnn] format
     */
    private static parseTime(str: string): bigint | null {
        // Match HH:MM:SS or HH:MM:SS.fractional
        const match = str.match(/^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$/);
        if (!match) {
            return null;
        }

        const hour = parseInt(match[1], 10);
        const minute = parseInt(match[2], 10);
        const second = parseInt(match[3], 10);
        
        // Parse fractional seconds if present
        let nano = 0;
        if (match[4]) {
            // Pad or truncate to 9 digits
            const fracStr = match[4].padEnd(9, '0').substring(0, 9);
            nano = parseInt(fracStr, 10);
        }

        // Validate ranges
        if (hour < 0 || hour > 23) {
            return null;
        }
        if (minute < 0 || minute > 59) {
            return null;
        }
        if (second < 0 || second > 59) {
            return null;
        }

        const nanos = BigInt(hour) * TimeValue.NANOS_PER_HOUR +
                     BigInt(minute) * TimeValue.NANOS_PER_MINUTE +
                     BigInt(second) * TimeValue.NANOS_PER_SECOND +
                     BigInt(nano);

        return nanos;
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}