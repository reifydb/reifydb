/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {BooleanValue} from '../../src';

describe('BooleanValue', () => {
    describe('constructor', () => {
        it('should create instance with true value', () => {
            const bool = new BooleanValue(true);
            expect(bool.value).toBe(true);
            expect(bool.type).toBe('Boolean');
        });

        it('should create instance with false value', () => {
            const bool = new BooleanValue(false);
            expect(bool.value).toBe(false);
            expect(bool.type).toBe('Boolean');
        });

        it('should create instance with undefined value', () => {
            const bool = new BooleanValue(undefined);
            expect(bool.value).toBeUndefined();
            expect(bool.type).toBe('Boolean');
        });

        it('should create instance with no arguments', () => {
            const bool = new BooleanValue();
            expect(bool.value).toBeUndefined();
            expect(bool.type).toBe('Boolean');
        });

        it('should throw error for non-boolean value', () => {
            expect(() => new BooleanValue(1 as any)).toThrow('Boolean value must be a boolean, got number');
            expect(() => new BooleanValue("true" as any)).toThrow('Boolean value must be a boolean, got string');
        });
    });

    describe('parse', () => {
        it('should parse "true" string', () => {
            const bool = BooleanValue.parse('true');
            expect(bool.value).toBe(true);
        });

        it('should parse "false" string', () => {
            const bool = BooleanValue.parse('false');
            expect(bool.value).toBe(false);
        });

        it('should parse "TRUE" string (case insensitive)', () => {
            const bool = BooleanValue.parse('TRUE');
            expect(bool.value).toBe(true);
        });

        it('should parse "FALSE" string (case insensitive)', () => {
            const bool = BooleanValue.parse('FALSE');
            expect(bool.value).toBe(false);
        });

        it('should trim whitespace', () => {
            const bool = BooleanValue.parse('  true  ');
            expect(bool.value).toBe(true);
        });

        it('should return undefined for empty string', () => {
            const bool = BooleanValue.parse('');
            expect(bool.value).toBeUndefined();
        });

        it('should return undefined for whitespace-only string', () => {
            const bool = BooleanValue.parse('   ');
            expect(bool.value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            const bool = BooleanValue.parse('⟪undefined⟫');
            expect(bool.value).toBeUndefined();
        });

        it('should throw error for invalid string', () => {
            expect(() => BooleanValue.parse('maybe')).toThrow('Cannot parse "maybe" as Boolean');
            expect(() => BooleanValue.parse('2')).toThrow('Cannot parse "2" as Boolean');
            expect(() => BooleanValue.parse('truee')).toThrow('Cannot parse "truee" as Boolean');
        });
    });

    describe('valueOf', () => {
        it('should return true', () => {
            const bool = new BooleanValue(true);
            expect(bool.valueOf()).toBe(true);
        });

        it('should return false', () => {
            const bool = new BooleanValue(false);
            expect(bool.valueOf()).toBe(false);
        });

        it('should return undefined when value is undefined', () => {
            const bool = new BooleanValue(undefined);
            expect(bool.valueOf()).toBeUndefined();
        });
    });
});