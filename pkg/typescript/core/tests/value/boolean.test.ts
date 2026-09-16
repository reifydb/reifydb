// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
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

        it('should reject undefined', () => {
            // a non-option value must always be defined, a none is carried by NoneValue
            expect(() => new BooleanValue(undefined)).toThrow();
        });

        it('should reject no arguments', () => {
            // a non-option value must always be defined, a none is carried by NoneValue
            expect(() => new BooleanValue()).toThrow();
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

        it('should reject an empty string', () => {
            // a non-option value must always be defined, so blank text is not a value
            expect(() => BooleanValue.parse('')).toThrow();
        });

        it('should reject a whitespace-only string', () => {
            // a non-option value must always be defined, so blank text is not a value
            expect(() => BooleanValue.parse('   ')).toThrow();
        });

        it('should reject the none marker', () => {
            // a non-option value must always be defined, so the none marker is not a value
            expect(() => BooleanValue.parse('⟪none⟫')).toThrow();
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

    });
});