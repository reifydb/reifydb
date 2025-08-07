/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Bool} from '../../src/value/bool';

describe('Bool', () => {
    describe('constructor', () => {
        it('should create instance with true value', () => {
            const bool = new Bool(true);
            expect(bool.value).toBe(true);
            expect(bool.type).toBe('Bool');
        });

        it('should create instance with false value', () => {
            const bool = new Bool(false);
            expect(bool.value).toBe(false);
            expect(bool.type).toBe('Bool');
        });

        it('should create instance with undefined value', () => {
            const bool = new Bool(undefined);
            expect(bool.value).toBeUndefined();
            expect(bool.type).toBe('Bool');
        });

        it('should create instance with no arguments', () => {
            const bool = new Bool();
            expect(bool.value).toBeUndefined();
            expect(bool.type).toBe('Bool');
        });

        it('should throw error for non-boolean value', () => {
            expect(() => new Bool(1 as any)).toThrow('Bool value must be a boolean, got number');
            expect(() => new Bool("true" as any)).toThrow('Bool value must be a boolean, got string');
        });
    });

    describe('parse', () => {
        it('should parse "true" string', () => {
            const bool = Bool.parse('true');
            expect(bool.value).toBe(true);
        });

        it('should parse "false" string', () => {
            const bool = Bool.parse('false');
            expect(bool.value).toBe(false);
        });

        it('should parse "TRUE" string (case insensitive)', () => {
            const bool = Bool.parse('TRUE');
            expect(bool.value).toBe(true);
        });

        it('should parse "FALSE" string (case insensitive)', () => {
            const bool = Bool.parse('FALSE');
            expect(bool.value).toBe(false);
        });

        it('should trim whitespace', () => {
            const bool = Bool.parse('  true  ');
            expect(bool.value).toBe(true);
        });

        it('should return undefined for empty string', () => {
            const bool = Bool.parse('');
            expect(bool.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const bool = Bool.parse('   ');
            expect(bool.value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            const bool = Bool.parse('⟪undefined⟫');
            expect(bool.value).toBeUndefined();
        });

        it('should throw error for invalid string', () => {
            expect(() => Bool.parse('maybe')).toThrow('Cannot parse "maybe" as Bool');
            expect(() => Bool.parse('2')).toThrow('Cannot parse "2" as Bool');
            expect(() => Bool.parse('truee')).toThrow('Cannot parse "truee" as Bool');
        });
    });

    describe('valueOf', () => {
        it('should return true', () => {
            const bool = new Bool(true);
            expect(bool.valueOf()).toBe(true);
        });

        it('should return false', () => {
            const bool = new Bool(false);
            expect(bool.valueOf()).toBe(false);
        });

        it('should return undefined when value is undefined', () => {
            const bool = new Bool(undefined);
            expect(bool.valueOf()).toBeUndefined();
        });
    });
});