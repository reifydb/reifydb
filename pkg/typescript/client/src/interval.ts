/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

/**
 * Represents an ISO 8601 duration/interval with nanosecond precision
 * Supports parsing and manipulation of time intervals
 */
export class Interval {
    private readonly _totalNanos: bigint;

    /**
     * Creates an Interval from total nanoseconds
     * @param totalNanos Total nanoseconds as bigint
     */
    constructor(totalNanos: bigint) {
        this._totalNanos = totalNanos;
    }

    /**
     * Parse ISO 8601 duration format (P1D, PT2H30M, P1Y2M3DT4H5M6S) to Interval
     * @param value ISO 8601 duration string
     * @returns Interval instance
     */
    static parse(value: string): Interval {
        if (!value.startsWith('P')) {
            throw new Error('Invalid interval format - must start with P');
        }
        
        let totalNanos = 0n;
        let currentNumber = '';
        let inTimePart = false;
        
        for (let i = 1; i < value.length; i++) {
            const char = value[i];
            
            if (char === 'T') {
                inTimePart = true;
                continue;
            }
            
            if (char >= '0' && char <= '9') {
                currentNumber += char;
                continue;
            }
            
            const num = BigInt(currentNumber);
            currentNumber = '';
            
            switch (char) {
                case 'Y':
                    if (inTimePart) throw new Error('Years not allowed in time part');
                    totalNanos += num * 365n * 24n * 60n * 60n * 1_000_000_000n; // Approximate
                    break;
                case 'M':
                    if (inTimePart) {
                        totalNanos += num * 60n * 1_000_000_000n; // Minutes
                    } else {
                        totalNanos += num * 30n * 24n * 60n * 60n * 1_000_000_000n; // Months (approximate)
                    }
                    break;
                case 'W':
                    if (inTimePart) throw new Error('Weeks not allowed in time part');
                    totalNanos += num * 7n * 24n * 60n * 60n * 1_000_000_000n;
                    break;
                case 'D':
                    if (inTimePart) throw new Error('Days not allowed in time part');
                    totalNanos += num * 24n * 60n * 60n * 1_000_000_000n;
                    break;
                case 'H':
                    if (!inTimePart) throw new Error('Hours only allowed in time part');
                    totalNanos += num * 60n * 60n * 1_000_000_000n;
                    break;
                case 'S':
                    if (!inTimePart) throw new Error('Seconds only allowed in time part');
                    totalNanos += num * 1_000_000_000n;
                    break;
                default:
                    throw new Error(`Invalid character in interval: ${char}`);
            }
        }
        
        if (currentNumber) {
            throw new Error('Incomplete interval specification');
        }
        
        return new Interval(totalNanos);
    }

    /**
     * Create an Interval from individual components
     * @param options Components of the interval
     * @returns Interval instance
     */
    static from(options: {
        years?: number;
        months?: number;
        weeks?: number;
        days?: number;
        hours?: number;
        minutes?: number;
        seconds?: number;
        milliseconds?: number;
        microseconds?: number;
        nanoseconds?: number;
    }): Interval {
        let totalNanos = 0n;
        
        if (options.years) totalNanos += BigInt(options.years) * 365n * 24n * 60n * 60n * 1_000_000_000n;
        if (options.months) totalNanos += BigInt(options.months) * 30n * 24n * 60n * 60n * 1_000_000_000n;
        if (options.weeks) totalNanos += BigInt(options.weeks) * 7n * 24n * 60n * 60n * 1_000_000_000n;
        if (options.days) totalNanos += BigInt(options.days) * 24n * 60n * 60n * 1_000_000_000n;
        if (options.hours) totalNanos += BigInt(options.hours) * 60n * 60n * 1_000_000_000n;
        if (options.minutes) totalNanos += BigInt(options.minutes) * 60n * 1_000_000_000n;
        if (options.seconds) totalNanos += BigInt(options.seconds) * 1_000_000_000n;
        if (options.milliseconds) totalNanos += BigInt(options.milliseconds) * 1_000_000n;
        if (options.microseconds) totalNanos += BigInt(options.microseconds) * 1_000n;
        if (options.nanoseconds) totalNanos += BigInt(options.nanoseconds);
        
        return new Interval(totalNanos);
    }

    /**
     * Get total nanoseconds
     * @returns Total nanoseconds as bigint
     */
    get totalNanoseconds(): bigint {
        return this._totalNanos;
    }

    /**
     * Get total microseconds (truncated)
     * @returns Total microseconds as bigint
     */
    get totalMicroseconds(): bigint {
        return this._totalNanos / 1_000n;
    }

    /**
     * Get total milliseconds (truncated)
     * @returns Total milliseconds as bigint
     */
    get totalMilliseconds(): bigint {
        return this._totalNanos / 1_000_000n;
    }

    /**
     * Get total seconds (truncated)
     * @returns Total seconds as bigint
     */
    get totalSeconds(): bigint {
        return this._totalNanos / 1_000_000_000n;
    }

    /**
     * Get total minutes (truncated)
     * @returns Total minutes as bigint
     */
    get totalMinutes(): bigint {
        return this._totalNanos / (60n * 1_000_000_000n);
    }

    /**
     * Get total hours (truncated)
     * @returns Total hours as bigint
     */
    get totalHours(): bigint {
        return this._totalNanos / (60n * 60n * 1_000_000_000n);
    }

    /**
     * Get total days (truncated)
     * @returns Total days as bigint
     */
    get totalDays(): bigint {
        return this._totalNanos / (24n * 60n * 60n * 1_000_000_000n);
    }

    /**
     * Get the components of the interval
     * @returns Object with days, hours, minutes, seconds, nanoseconds
     */
    get components(): {
        days: bigint;
        hours: bigint;
        minutes: bigint;
        seconds: bigint;
        nanoseconds: bigint;
    } {
        const totalNanos = this._totalNanos;
        const days = totalNanos / (24n * 60n * 60n * 1_000_000_000n);
        const remainder1 = totalNanos % (24n * 60n * 60n * 1_000_000_000n);
        const hours = remainder1 / (60n * 60n * 1_000_000_000n);
        const remainder2 = remainder1 % (60n * 60n * 1_000_000_000n);
        const minutes = remainder2 / (60n * 1_000_000_000n);
        const remainder3 = remainder2 % (60n * 1_000_000_000n);
        const seconds = remainder3 / 1_000_000_000n;
        const nanoseconds = remainder3 % 1_000_000_000n;

        return { days, hours, minutes, seconds, nanoseconds };
    }

    /**
     * Convert to ISO 8601 duration string
     * @returns ISO 8601 duration string
     */
    toString(): string {
        if (this._totalNanos === 0n) return 'PT0S';
        
        const { days, hours, minutes, seconds, nanoseconds } = this.components;
        
        let result = 'P';
        
        if (days > 0n) {
            result += `${days}D`;
        }
        
        if (hours > 0n || minutes > 0n || seconds > 0n || nanoseconds > 0n) {
            result += 'T';
            
            if (hours > 0n) {
                result += `${hours}H`;
            }
            
            if (minutes > 0n) {
                result += `${minutes}M`;
            }
            
            if (seconds > 0n || nanoseconds > 0n) {
                if (nanoseconds > 0n) {
                    const totalSecondsWithNanos = Number(seconds) + Number(nanoseconds) / 1_000_000_000;
                    result += `${totalSecondsWithNanos}S`;
                } else {
                    result += `${seconds}S`;
                }
            }
        }
        
        return result;
    }

    /**
     * Convert to JSON representation
     * @returns JSON-serializable object
     */
    toJSON(): {
        totalNanoseconds: string;
        iso8601: string;
    } {
        return {
            totalNanoseconds: this._totalNanos.toString(),
            iso8601: this.toString()
        };
    }

    /**
     * Add another interval to this interval
     * @param other Another interval
     * @returns New interval with the sum
     */
    add(other: Interval): Interval {
        return new Interval(this._totalNanos + other._totalNanos);
    }

    /**
     * Subtract another interval from this interval
     * @param other Another interval
     * @returns New interval with the difference
     */
    subtract(other: Interval): Interval {
        return new Interval(this._totalNanos - other._totalNanos);
    }

    /**
     * Check if this interval equals another interval
     * @param other Another interval
     * @returns True if intervals are equal
     */
    equals(other: Interval): boolean {
        return this._totalNanos === other._totalNanos;
    }

    /**
     * Check if this interval is greater than another interval
     * @param other Another interval
     * @returns True if this interval is greater
     */
    greaterThan(other: Interval): boolean {
        return this._totalNanos > other._totalNanos;
    }

    /**
     * Check if this interval is less than another interval
     * @param other Another interval
     * @returns True if this interval is less
     */
    lessThan(other: Interval): boolean {
        return this._totalNanos < other._totalNanos;
    }
}