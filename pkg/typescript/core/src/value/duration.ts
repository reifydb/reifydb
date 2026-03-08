// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import { Type, Value, TypeValuePair } from ".";
import { NONE_VALUE } from "../constant";

/**
 * A duration value representing a time span or elapsed time.
 * Internally stored as months, days, and nanoseconds.
 */
export class DurationValue implements Value {
    readonly type: Type = "Duration" as const;
    private readonly months?: number;  // years*12 + months
    private readonly days?: number;    // separate days (don't normalize due to variable month length)
    private readonly nanos?: bigint;   // all time components as nanoseconds

    constructor(value?: { months: number; days: number; nanos: bigint } | string) {
        if (value !== undefined) {
            if (typeof value === 'string') {
                // Parse ISO 8601 duration format
                const parsed = DurationValue.parseDuration(value);
                if (!parsed) {
                    throw new Error(`Invalid duration string: ${value}`);
                }
                this.months = parsed.months;
                this.days = parsed.days;
                this.nanos = parsed.nanos;
            } else if (typeof value === 'object' && value !== null) {
                this.months = value.months;
                this.days = value.days;
                this.nanos = value.nanos;
            } else {
                throw new Error(`Duration value must be an object or string, got ${typeof value}`);
            }
        } else {
            this.months = undefined;
            this.days = undefined;
            this.nanos = undefined;
        }
    }

    /**
     * Create a new duration from months, days, and nanoseconds
     */
    static new(months: number, days: number, nanos: bigint): DurationValue {
        return new DurationValue({ months, days, nanos });
    }

    /**
     * Create a duration from seconds
     */
    static fromSeconds(seconds: number): DurationValue {
        return new DurationValue({ months: 0, days: 0, nanos: BigInt(seconds) * 1_000_000_000n });
    }

    /**
     * Create a duration from milliseconds
     */
    static fromMilliseconds(milliseconds: number): DurationValue {
        return new DurationValue({ months: 0, days: 0, nanos: BigInt(milliseconds) * 1_000_000n });
    }

    /**
     * Create a duration from microseconds
     */
    static fromMicroseconds(microseconds: number): DurationValue {
        return new DurationValue({ months: 0, days: 0, nanos: BigInt(microseconds) * 1_000n });
    }

    /**
     * Create a duration from nanoseconds
     */
    static fromNanoseconds(nanoseconds: bigint): DurationValue {
        return new DurationValue({ months: 0, days: 0, nanos: nanoseconds });
    }

    /**
     * Create a duration from minutes
     */
    static fromMinutes(minutes: number): DurationValue {
        return new DurationValue({ months: 0, days: 0, nanos: BigInt(minutes) * 60n * 1_000_000_000n });
    }

    /**
     * Create a duration from hours
     */
    static fromHours(hours: number): DurationValue {
        return new DurationValue({ months: 0, days: 0, nanos: BigInt(hours) * 60n * 60n * 1_000_000_000n });
    }

    /**
     * Create a duration from days
     */
    static fromDays(days: number): DurationValue {
        return new DurationValue({ months: 0, days, nanos: 0n });
    }

    /**
     * Create a duration from weeks
     */
    static fromWeeks(weeks: number): DurationValue {
        return new DurationValue({ months: 0, days: weeks * 7, nanos: 0n });
    }

    /**
     * Create a duration from months
     */
    static fromMonths(months: number): DurationValue {
        return new DurationValue({ months, days: 0, nanos: 0n });
    }

    /**
     * Create a duration from years
     */
    static fromYears(years: number): DurationValue {
        return new DurationValue({ months: years * 12, days: 0, nanos: 0n });
    }

    /**
     * Create a zero duration
     */
    static zero(): DurationValue {
        return new DurationValue({ months: 0, days: 0, nanos: 0n });
    }

    /**
     * Get default duration (zero)
     */
    static default(): DurationValue {
        return DurationValue.zero();
    }

    /**
     * Parse a duration string in ISO 8601 duration format
     */
    static parse(str: string): DurationValue {
        const trimmed = str.trim();
        
        if (trimmed === '' || trimmed === NONE_VALUE) {
            return new DurationValue(undefined);
        }

        const parsed = DurationValue.parseDuration(trimmed);
        if (!parsed) {
            throw new Error(`Cannot parse "${str}" as Duration`);
        }

        return new DurationValue({ months: parsed.months, days: parsed.days, nanos: parsed.nanos });
    }

    /**
     * Get total seconds (truncated)
     */
    seconds(): bigint | undefined {
        if (this.nanos === undefined) return undefined;
        return this.nanos / 1_000_000_000n;
    }

    /**
     * Get total milliseconds (truncated)
     */
    milliseconds(): bigint | undefined {
        if (this.nanos === undefined) return undefined;
        return this.nanos / 1_000_000n;
    }

    /**
     * Get total microseconds (truncated)
     */
    microseconds(): bigint | undefined {
        if (this.nanos === undefined) return undefined;
        return this.nanos / 1_000n;
    }

    /**
     * Get total nanoseconds
     */
    nanoseconds(): bigint | undefined {
        return this.nanos;
    }

    /**
     * Get months component
     */
    getMonths(): number | undefined {
        return this.months;
    }

    /**
     * Get days component
     */
    getDays(): number | undefined {
        return this.days;
    }

    /**
     * Get nanoseconds component
     */
    getNanos(): bigint | undefined {
        return this.nanos;
    }

    /**
     * Check if duration is positive (any component > 0)
     */
    isPositive(): boolean {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return false;
        }
        return this.months > 0 || this.days > 0 || this.nanos > 0n;
    }

    /**
     * Check if duration is negative (any component < 0)
     */
    isNegative(): boolean {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return false;
        }
        return this.months < 0 || this.days < 0 || this.nanos < 0n;
    }

    /**
     * Get absolute value of duration
     */
    abs(): DurationValue {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return new DurationValue(undefined);
        }
        return new DurationValue({
            months: Math.abs(this.months),
            days: Math.abs(this.days),
            nanos: this.nanos < 0n ? -this.nanos : this.nanos
        });
    }

    /**
     * Negate the duration
     */
    negate(): DurationValue {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return new DurationValue(undefined);
        }
        return new DurationValue({
            months: -this.months,
            days: -this.days,
            nanos: -this.nanos
        });
    }

    /**
     * Format as ISO 8601 duration string
     */
    toIsoString(): string {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return 'none';
        }

        // Handle zero duration
        if (this.months === 0 && this.days === 0 && this.nanos === 0n) {
            return 'PT0S';
        }

        let result = 'P';

        // Extract years and months
        const years = Math.floor(this.months / 12);
        const months = this.months % 12;

        if (years !== 0) {
            result += `${years}Y`;
        }

        if (months !== 0) {
            result += `${months}M`;
        }

        // Time components from nanos with normalization
        const totalSeconds = this.nanos / 1_000_000_000n;
        const remainingNanos = this.nanos % 1_000_000_000n;

        // Normalize to days if hours >= 24
        const extraDays = totalSeconds / 86400n; // 24 * 60 * 60
        const remainingSeconds = totalSeconds % 86400n;

        const displayDays = this.days + Number(extraDays);
        const hours = remainingSeconds / 3600n;
        const minutes = (remainingSeconds % 3600n) / 60n;
        const seconds = remainingSeconds % 60n;

        if (displayDays !== 0) {
            result += `${displayDays}D`;
        }

        // Add time components if any
        if (hours !== 0n || minutes !== 0n || seconds !== 0n || remainingNanos !== 0n) {
            result += 'T';

            if (hours !== 0n) {
                result += `${hours}H`;
            }

            if (minutes !== 0n) {
                result += `${minutes}M`;
            }

            if (seconds !== 0n || remainingNanos !== 0n) {
                if (remainingNanos !== 0n) {
                    // Format fractional seconds with trailing zeros removed
                    const fractional = Number(remainingNanos) / 1_000_000_000;
                    const totalSecondsFloat = Number(seconds) + fractional;
                    // Format with 9 decimal places then remove trailing zeros
                    const formatted = totalSecondsFloat.toFixed(9).replace(/0+$/, '').replace(/\.$/, '');
                    result += `${formatted}S`;
                } else {
                    result += `${seconds}S`;
                }
            }
        }

        return result;
    }

    /**
     * Format as compact human-readable duration string
     * Examples: "1y2mo3d4h5m6s", "1d2h", "30s", "500ms", "0s"
     */
    toString(): string {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return 'none';
        }

        if (this.months === 0 && this.days === 0 && this.nanos === 0n) {
            return '0s';
        }

        let result = '';

        const years = Math.floor(this.months / 12);
        const months = this.months % 12;

        const totalSeconds = this.nanos / 1_000_000_000n;
        const remainingNanos = this.nanos % 1_000_000_000n;

        const extraDays = totalSeconds / 86400n;
        const remainingSeconds = totalSeconds % 86400n;

        const displayDays = this.days + Number(extraDays);
        const hours = remainingSeconds / 3600n;
        const minutes = (remainingSeconds % 3600n) / 60n;
        const seconds = remainingSeconds % 60n;

        const absRemaining = remainingNanos < 0n ? -remainingNanos : remainingNanos;
        const ms = absRemaining / 1_000_000n;
        const us = (absRemaining % 1_000_000n) / 1_000n;
        const ns = absRemaining % 1_000n;

        if (years !== 0) result += `${years}y`;
        if (months !== 0) result += `${months}mo`;
        if (displayDays !== 0) result += `${displayDays}d`;
        if (hours !== 0n) result += `${hours}h`;
        if (minutes !== 0n) result += `${minutes}m`;
        if (seconds !== 0n) result += `${seconds}s`;

        if (ms !== 0n || us !== 0n || ns !== 0n) {
            if (remainingNanos < 0n && seconds === 0n && hours === 0n && minutes === 0n
                && displayDays === 0 && years === 0 && months === 0) {
                result += '-';
            }
            if (ms !== 0n) result += `${ms}ms`;
            if (us !== 0n) result += `${us}us`;
            if (ns !== 0n) result += `${ns}ns`;
        }

        return result;
    }

    valueOf(): { months: number; days: number; nanos: bigint } | undefined {
        if (this.months === undefined || this.days === undefined || this.nanos === undefined) {
            return undefined;
        }
        return { months: this.months, days: this.days, nanos: this.nanos };
    }

    /**
     * Get the internal representation
     */
    get value(): { months: number; days: number; nanos: bigint } | undefined {
        return this.valueOf();
    }

    /**
     * Helper to parse duration format (human-readable or ISO 8601)
     */
    private static parseDuration(str: string): { months: number; days: number; nanos: bigint } | null {
        const negative = str.startsWith('-');
        const cleanStr = negative ? str.substring(1) : str;

        // Try ISO 8601 format first (starts with P)
        if (cleanStr.startsWith('P')) {
            return DurationValue.parseIsoDuration(cleanStr, negative);
        }

        // Try human-readable format: 1y2mo3d4h5m6s500ms100us50ns
        return DurationValue.parseHumanDuration(cleanStr, negative);
    }

    /**
     * Parse ISO 8601 duration format: P[n]Y[n]M[n]DT[n]H[n]M[n.n]S
     */
    private static parseIsoDuration(str: string, negative: boolean): { months: number; days: number; nanos: bigint } | null {
        const match = str.match(/^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/);
        if (!match) {
            return null;
        }

        // Check if the match has at least one value
        if (!match[1] && !match[2] && !match[3] && !match[4] && !match[5] && !match[6]) {
            return null; // Invalid: P without any values
        }

        const years = parseInt(match[1] || '0', 10);
        const months = parseInt(match[2] || '0', 10);
        const days = parseInt(match[3] || '0', 10);
        const hours = parseInt(match[4] || '0', 10);
        const minutes = parseInt(match[5] || '0', 10);
        const secondsStr = match[6] || '0';

        // Parse seconds and fractional seconds
        const secondsParts = secondsStr.split('.');
        const wholeSeconds = parseInt(secondsParts[0] || '0', 10);
        let fracNanos = 0n;
        if (secondsParts.length > 1) {
            // Pad fractional part to 9 digits
            const fracStr = secondsParts[1].padEnd(9, '0').substring(0, 9);
            fracNanos = BigInt(fracStr);
        }

        // Calculate total nanoseconds
        const totalNanos = BigInt(hours) * 3600n * 1_000_000_000n +
                          BigInt(minutes) * 60n * 1_000_000_000n +
                          BigInt(wholeSeconds) * 1_000_000_000n +
                          fracNanos;

        const totalMonths = years * 12 + months;

        if (negative) {
            return { months: -totalMonths, days: -days, nanos: -totalNanos };
        }
        return { months: totalMonths, days: days, nanos: totalNanos };
    }

    /**
     * Parse human-readable duration format: 1y2mo3d4h5m6s500ms100us50ns
     */
    private static parseHumanDuration(str: string, negative: boolean): { months: number; days: number; nanos: bigint } | null {
        // Match components: years, months, days, hours, minutes, seconds, ms, us, ns
        // Order matters: 'mo' before 'm', 'ms' before 'm', 'us' before 'u', 'ns' before 'n'
        const pattern = /^(?:(\d+)y)?(?:(\d+)mo)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m(?!s|o))?(?:(\d+)s(?!$)?)?(?:(\d+)ms)?(?:(\d+)us)?(?:(\d+)ns)?$/;
        const match = str.match(pattern);
        if (!match) {
            return null;
        }

        // Check if the match has at least one value
        if (!match[1] && !match[2] && !match[3] && !match[4] && !match[5] && !match[6] && !match[7] && !match[8] && !match[9]) {
            return null;
        }

        const years = parseInt(match[1] || '0', 10);
        const months = parseInt(match[2] || '0', 10);
        const days = parseInt(match[3] || '0', 10);
        const hours = parseInt(match[4] || '0', 10);
        const minutes = parseInt(match[5] || '0', 10);
        const seconds = parseInt(match[6] || '0', 10);
        const ms = parseInt(match[7] || '0', 10);
        const us = parseInt(match[8] || '0', 10);
        const ns = parseInt(match[9] || '0', 10);

        const totalNanos = BigInt(hours) * 3600n * 1_000_000_000n +
                          BigInt(minutes) * 60n * 1_000_000_000n +
                          BigInt(seconds) * 1_000_000_000n +
                          BigInt(ms) * 1_000_000n +
                          BigInt(us) * 1_000n +
                          BigInt(ns);

        const totalMonths = years * 12 + months;

        if (negative) {
            return { months: -totalMonths, days: -days, nanos: -totalNanos };
        }
        return { months: totalMonths, days: days, nanos: totalNanos };
    }

    /**
     * Compare two durations for equality
     */
    equals(other: Value): boolean {
        if (other.type !== this.type) {
            return false;
        }
        
        const otherDuration = other as DurationValue;
        if (this.months === undefined || otherDuration.months === undefined) {
            return this.months === otherDuration.months && 
                   this.days === otherDuration.days && 
                   this.nanos === otherDuration.nanos;
        }
        
        return this.months === otherDuration.months && 
               this.days === otherDuration.days && 
               this.nanos === otherDuration.nanos;
    }

    toJSON(): string {
        return this.value === undefined ? NONE_VALUE : this.toIsoString();
    }

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? NONE_VALUE : this.toIsoString()
        };
    }
}