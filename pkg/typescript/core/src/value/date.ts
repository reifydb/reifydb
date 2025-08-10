/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {Type, Value, TypeValuePair} from "./type";
import {UNDEFINED_VALUE} from "../constant";

/**
 * A date value representing a calendar date (year, month, day) without time information.
 * Always interpreted in UTC.
 * Internally stored as months and days.
 */
export class DateValue implements Value {
    readonly type: Type = "Date" as const;
    private readonly months?: number; // years*12 + months
    private readonly days?: number;   // day of month (1-31)

    constructor(value?: Date | string | number) {
        if (value !== undefined) {
            if (value instanceof Date) {
                // Remove time component - set to UTC midnight
                const year = value.getUTCFullYear();
                const month = value.getUTCMonth() + 1; // Convert to 1-based
                const day = value.getUTCDate();
                
                this.months = year * 12 + (month - 1);
                this.days = day;
            } else if (typeof value === 'string') {
                // Parse YYYY-MM-DD format
                const parsed = DateValue.parseDate(value);
                if (!parsed) {
                    throw new Error(`Invalid date string: ${value}`);
                }
                this.months = parsed.months;
                this.days = parsed.days;
            } else if (typeof value === 'number') {
                // Interpret as days since epoch
                const date = DateValue.fromDaysSinceEpochToComponents(value);
                if (!date) {
                    throw new Error(`Invalid days since epoch: ${value}`);
                }
                this.months = date.months;
                this.days = date.days;
            } else {
                throw new Error(`Date value must be a Date, string, or number, got ${typeof value}`);
            }
        } else {
            this.months = undefined;
            this.days = undefined;
        }
    }

    /**
     * Create a DateValue from year, month (1-12), and day (1-31)
     */
    static fromYMD(year: number, month: number, day: number): DateValue {
        // Validate the date is valid
        if (!DateValue.isValidDate(year, month, day)) {
            throw new Error(`Invalid date: ${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`);
        }
        
        const result = new DateValue(undefined);
        (result as any).months = year * 12 + (month - 1);
        (result as any).days = day;
        return result;
    }

    /**
     * Get today's date (in UTC)
     */
    static today(): DateValue {
        const now = new Date();
        return DateValue.fromYMD(
            now.getUTCFullYear(),
            now.getUTCMonth() + 1,
            now.getUTCDate()
        );
    }

    /**
     * Parse a date string in YYYY-MM-DD format
     */
    static parse(str: string): DateValue {
        const trimmed = str.trim();
        
        if (trimmed === '' || trimmed === UNDEFINED_VALUE) {
            return new DateValue(undefined);
        }

        const parsed = DateValue.parseDate(trimmed);
        if (!parsed) {
            throw new Error(`Cannot parse "${str}" as Date`);
        }

        const result = new DateValue(undefined);
        (result as any).months = parsed.months;
        (result as any).days = parsed.days;
        return result;
    }

    /**
     * Convert to days since Unix epoch (1970-01-01) for storage
     */
    toDaysSinceEpoch(): number | undefined {
        if (this.months === undefined || this.days === undefined) {
            return undefined;
        }
        
        const year = Math.floor(this.months / 12);
        // Handle negative months correctly
        let m = this.months % 12;
        if (m < 0) m += 12;
        const month = m + 1;
        
        // Create date with special handling for years < 100
        let date: Date;
        if (year >= 0 && year < 100) {
            date = new Date(Date.UTC(2000, month - 1, this.days));
            date.setUTCFullYear(year);
        } else {
            date = new Date(Date.UTC(year, month - 1, this.days));
        }
        
        const epoch = new Date(Date.UTC(1970, 0, 1));
        const diffMs = date.getTime() - epoch.getTime();
        return Math.floor(diffMs / (1000 * 60 * 60 * 24));
    }

    /**
     * Create from days since Unix epoch
     */
    static fromDaysSinceEpochToComponents(days: number): { months: number; days: number } | null {
        const epoch = new Date(Date.UTC(1970, 0, 1));
        const ms = days * 24 * 60 * 60 * 1000;
        const date = new Date(epoch.getTime() + ms);
        
        // Check for valid date
        if (!isFinite(date.getTime())) {
            return null;
        }
        
        const year = date.getUTCFullYear();
        const month = date.getUTCMonth() + 1;
        const day = date.getUTCDate();
        
        return {
            months: year * 12 + (month - 1),
            days: day
        };
    }

    /**
     * Get the year component
     */
    year(): number | undefined {
        if (this.months === undefined) return undefined;
        return Math.floor(this.months / 12);
    }

    /**
     * Get the month component (1-12)
     */
    month(): number | undefined {
        if (this.months === undefined) return undefined;
        // Handle negative months correctly
        let m = this.months % 12;
        if (m < 0) m += 12;
        return m + 1;
    }

    /**
     * Get the day component (1-31)
     */
    day(): number | undefined {
        return this.days;
    }

    /**
     * Format as YYYY-MM-DD string
     */
    toString(): string {
        if (this.months === undefined || this.days === undefined) {
            return 'undefined';
        }

        const year = Math.floor(this.months / 12);
        // Handle negative months correctly
        let m = this.months % 12;
        if (m < 0) m += 12;
        const month = String(m + 1).padStart(2, '0');
        const day = String(this.days).padStart(2, '0');
        
        // Handle negative years (BC dates)
        if (year < 0) {
            const absYear = Math.abs(year);
            return `-${String(absYear).padStart(4, '0')}-${month}-${day}`;
        } else {
            return `${String(year).padStart(4, '0')}-${month}-${day}`;
        }
    }

    valueOf(): Date | undefined {
        if (this.months === undefined || this.days === undefined) {
            return undefined;
        }
        
        const year = Math.floor(this.months / 12);
        // Handle negative months correctly
        let m = this.months % 12;
        if (m < 0) m += 12;
        const month = m + 1;
        
        // Handle years < 100 specially
        let date: Date;
        if (year >= 0 && year < 100) {
            date = new Date(Date.UTC(2000, month - 1, this.days));
            date.setUTCFullYear(year);
        } else {
            date = new Date(Date.UTC(year, month - 1, this.days));
        }
        
        return date;
    }
    
    /**
     * Get the internal representation
     */
    get value(): Date | undefined {
        return this.valueOf();
    }

    /**
     * Helper to parse YYYY-MM-DD format
     */
    private static parseDate(str: string): { months: number; days: number } | null {
        // Match YYYY-MM-DD format, including negative years
        const match = str.match(/^(-?\d{1,4})-(\d{2})-(\d{2})$/);
        if (!match) {
            return null;
        }

        const year = parseInt(match[1], 10);
        const month = parseInt(match[2], 10);
        const day = parseInt(match[3], 10);

        // Validate month and day ranges
        if (month < 1 || month > 12) {
            return null;
        }
        if (day < 1 || day > 31) {
            return null;
        }

        // Validate the date is valid (e.g., no Feb 30)
        if (!DateValue.isValidDate(year, month, day)) {
            return null;
        }

        return {
            months: year * 12 + (month - 1),
            days: day
        };
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

    encode(): TypeValuePair {
        return {
            type: this.type,
            value: this.value === undefined ? UNDEFINED_VALUE : this.toString()
        };
    }
}