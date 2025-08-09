/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {BoolValue} from '../../src/value/bool';

describe('BoolValue', () => {
    describe('constructor', () => {
        it('should create instance with true value', () => {
            const bool = new BoolValue(true);
            expect(bool.value).toBe(true);
            expect(bool.type).toBe('Bool');
        });

        it('should create instance with false value', () => {
            const bool = new BoolValue(false);
            expect(bool.value).toBe(false);
            expect(bool.type).toBe('Bool');
        });

        it('should create instance with undefined value', () => {
            const bool = new BoolValue(undefined);
            expect(bool.value).toBeUndefined();
            expect(bool.type).toBe('Bool');
        });

        it('should create instance with no arguments', () => {
            const bool = new BoolValue();
            expect(bool.value).toBeUndefined();
            expect(bool.type).toBe('Bool');
        });

        it('should throw error for non-boolean value', () => {
            expect(() => new BoolValue(1 as any)).toThrow('Bool value must be a boolean, got number');
            expect(() => new BoolValue("true" as any)).toThrow('Bool value must be a boolean, got string');
        });
    });

    describe('parse', () => {
        it('should parse "true" string', () => {
            const bool = BoolValue.parse('true');
            expect(bool.value).toBe(true);
        });

        it('should parse "false" string', () => {
            const bool = BoolValue.parse('false');
            expect(bool.value).toBe(false);
        });

        it('should parse "TRUE" string (case insensitive)', () => {
            const bool = BoolValue.parse('TRUE');
            expect(bool.value).toBe(true);
        });

        it('should parse "FALSE" string (case insensitive)', () => {
            const bool = BoolValue.parse('FALSE');
            expect(bool.value).toBe(false);
        });

        it('should trim whitespace', () => {
            const bool = BoolValue.parse('  true  ');
            expect(bool.value).toBe(true);
        });

        it('should return undefined for empty string', () => {
            const bool = BoolValue.parse('');
            expect(bool.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const bool = BoolValue.parse('   ');
            expect(bool.value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            const bool = BoolValue.parse('⟪undefined⟫');
            expect(bool.value).toBeUndefined();
        });

        it('should throw error for invalid string', () => {
            expect(() => BoolValue.parse('maybe')).toThrow('Cannot parse "maybe" as Bool');
            expect(() => BoolValue.parse('2')).toThrow('Cannot parse "2" as Bool');
            expect(() => BoolValue.parse('truee')).toThrow('Cannot parse "truee" as Bool');
        });
    });

    describe('valueOf', () => {
        it('should return true', () => {
            const bool = new BoolValue(true);
            expect(bool.valueOf()).toBe(true);
        });

        it('should return false', () => {
            const bool = new BoolValue(false);
            expect(bool.valueOf()).toBe(false);
        });

        it('should return undefined when value is undefined', () => {
            const bool = new BoolValue(undefined);
            expect(bool.valueOf()).toBeUndefined();
        });
    });
});