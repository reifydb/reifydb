// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {Utf8Value} from '../../src';

describe('Utf8Value', () => {
    describe('constructor', () => {
        it('should create instance with string value', () => {
            const utf8 = new Utf8Value('hello world');
            expect(utf8.value).toBe('hello world');
            expect(utf8.type).toBe('Utf8');
        });

        it('should create instance with empty string', () => {
            const utf8 = new Utf8Value('');
            expect(utf8.value).toBe('');
            expect(utf8.type).toBe('Utf8');
        });

        it('should create instance with undefined value', () => {
            const utf8 = new Utf8Value(undefined);
            expect(utf8.value).toBeUndefined();
            expect(utf8.type).toBe('Utf8');
        });

        it('should create instance with no arguments', () => {
            const utf8 = new Utf8Value();
            expect(utf8.value).toBeUndefined();
            expect(utf8.type).toBe('Utf8');
        });

        it('should handle UTF-8 characters', () => {
            const utf8 = new Utf8Value('你好世界 🌍 café');
            expect(utf8.value).toBe('你好世界 🌍 café');
        });

        it('should handle emojis', () => {
            const utf8 = new Utf8Value('😀😃😄😁');
            expect(utf8.value).toBe('😀😃😄😁');
        });

        it('should handle special characters', () => {
            const utf8 = new Utf8Value('!@#$%^&*()_+-=[]{}|;:\'",.<>?/`~');
            expect(utf8.value).toBe('!@#$%^&*()_+-=[]{}|;:\'",.<>?/`~');
        });

        it('should handle newlines and tabs', () => {
            const utf8 = new Utf8Value('line1\nline2\ttab');
            expect(utf8.value).toBe('line1\nline2\ttab');
        });

        it('should handle very long strings', () => {
            const longString = 'a'.repeat(10000);
            const utf8 = new Utf8Value(longString);
            expect(utf8.value).toBe(longString);
            expect(utf8.value?.length).toBe(10000);
        });

        it('should throw error for non-string value', () => {
            expect(() => new Utf8Value(123 as any)).toThrow('Utf8 value must be a string, got number');
            expect(() => new Utf8Value(true as any)).toThrow('Utf8 value must be a string, got boolean');
            expect(() => new Utf8Value({} as any)).toThrow('Utf8 value must be a string, got object');
        });
    });

    describe('parse', () => {
        it('should parse regular string', () => {
            const utf8 = Utf8Value.parse('hello world');
            expect(utf8.value).toBe('hello world');
        });

        it('should parse empty string', () => {
            const utf8 = Utf8Value.parse('');
            expect(utf8.value).toBe('');
        });

        it('should preserve whitespace', () => {
            const utf8 = Utf8Value.parse('  spaces  ');
            expect(utf8.value).toBe('  spaces  ');
        });

        it('should parse string with only whitespace', () => {
            const utf8 = Utf8Value.parse('   ');
            expect(utf8.value).toBe('   ');
        });

        it('should parse UTF-8 characters', () => {
            const utf8 = Utf8Value.parse('こんにちは世界 🌏 naïve');
            expect(utf8.value).toBe('こんにちは世界 🌏 naïve');
        });

        it('should parse emojis correctly', () => {
            const utf8 = Utf8Value.parse('🎉🎊🎈🎁');
            expect(utf8.value).toBe('🎉🎊🎈🎁');
        });

        it('should parse special characters', () => {
            const utf8 = Utf8Value.parse('\\n\\t\\r');
            expect(utf8.value).toBe('\\n\\t\\r');
        });

        it('should parse numbers as strings', () => {
            const utf8 = Utf8Value.parse('12345');
            expect(utf8.value).toBe('12345');
        });

        it('should parse boolean-like strings as strings', () => {
            const utf8True = Utf8Value.parse('true');
            expect(utf8True.value).toBe('true');
            
            const utf8False = Utf8Value.parse('false');
            expect(utf8False.value).toBe('false');
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            const utf8 = Utf8Value.parse('⟪undefined⟫');
            expect(utf8.value).toBeUndefined();
        });

        it('should not return undefined for string containing UNDEFINED_VALUE', () => {
            const utf8 = Utf8Value.parse('text ⟪undefined⟫ more text');
            expect(utf8.value).toBe('text ⟪undefined⟫ more text');
        });

        it('should handle multi-line strings', () => {
            const multiline = `line 1
line 2
line 3`;
            const utf8 = Utf8Value.parse(multiline);
            expect(utf8.value).toBe(multiline);
        });
    });

    describe('valueOf', () => {
        it('should return the string value', () => {
            const utf8 = new Utf8Value('test string');
            expect(utf8.valueOf()).toBe('test string');
        });

        it('should return empty string', () => {
            const utf8 = new Utf8Value('');
            expect(utf8.valueOf()).toBe('');
        });

        it('should return undefined when value is undefined', () => {
            const utf8 = new Utf8Value(undefined);
            expect(utf8.valueOf()).toBeUndefined();
        });

        it('should return UTF-8 string', () => {
            const utf8 = new Utf8Value('文字列 🚀');
            expect(utf8.valueOf()).toBe('文字列 🚀');
        });
    });
});