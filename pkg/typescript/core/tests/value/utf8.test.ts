/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {Utf8} from '../../src/value/utf8';

describe('Utf8', () => {
    describe('constructor', () => {
        it('should create instance with string value', () => {
            const utf8 = new Utf8('hello world');
            expect(utf8.value).toBe('hello world');
            expect(utf8.type).toBe('Utf8');
        });

        it('should create instance with empty string', () => {
            const utf8 = new Utf8('');
            expect(utf8.value).toBe('');
            expect(utf8.type).toBe('Utf8');
        });

        it('should create instance with undefined value', () => {
            const utf8 = new Utf8(undefined);
            expect(utf8.value).toBeUndefined();
            expect(utf8.type).toBe('Utf8');
        });

        it('should create instance with no arguments', () => {
            const utf8 = new Utf8();
            expect(utf8.value).toBeUndefined();
            expect(utf8.type).toBe('Utf8');
        });

        it('should handle UTF-8 characters', () => {
            const utf8 = new Utf8('你好世界 🌍 café');
            expect(utf8.value).toBe('你好世界 🌍 café');
        });

        it('should handle emojis', () => {
            const utf8 = new Utf8('😀😃😄😁');
            expect(utf8.value).toBe('😀😃😄😁');
        });

        it('should handle special characters', () => {
            const utf8 = new Utf8('!@#$%^&*()_+-=[]{}|;:\'",.<>?/`~');
            expect(utf8.value).toBe('!@#$%^&*()_+-=[]{}|;:\'",.<>?/`~');
        });

        it('should handle newlines and tabs', () => {
            const utf8 = new Utf8('line1\nline2\ttab');
            expect(utf8.value).toBe('line1\nline2\ttab');
        });

        it('should handle very long strings', () => {
            const longString = 'a'.repeat(10000);
            const utf8 = new Utf8(longString);
            expect(utf8.value).toBe(longString);
            expect(utf8.value?.length).toBe(10000);
        });

        it('should throw error for non-string value', () => {
            expect(() => new Utf8(123 as any)).toThrow('Utf8 value must be a string, got number');
            expect(() => new Utf8(true as any)).toThrow('Utf8 value must be a string, got boolean');
            expect(() => new Utf8({} as any)).toThrow('Utf8 value must be a string, got object');
        });
    });

    describe('parse', () => {
        it('should parse regular string', () => {
            const utf8 = Utf8.parse('hello world');
            expect(utf8.value).toBe('hello world');
        });

        it('should parse empty string', () => {
            const utf8 = Utf8.parse('');
            expect(utf8.value).toBe('');
        });

        it('should preserve whitespace', () => {
            const utf8 = Utf8.parse('  spaces  ');
            expect(utf8.value).toBe('  spaces  ');
        });

        it('should parse string with only whitespace', () => {
            const utf8 = Utf8.parse('   ');
            expect(utf8.value).toBe('   ');
        });

        it('should parse UTF-8 characters', () => {
            const utf8 = Utf8.parse('こんにちは世界 🌏 naïve');
            expect(utf8.value).toBe('こんにちは世界 🌏 naïve');
        });

        it('should parse emojis correctly', () => {
            const utf8 = Utf8.parse('🎉🎊🎈🎁');
            expect(utf8.value).toBe('🎉🎊🎈🎁');
        });

        it('should parse special characters', () => {
            const utf8 = Utf8.parse('\\n\\t\\r');
            expect(utf8.value).toBe('\\n\\t\\r');
        });

        it('should parse numbers as strings', () => {
            const utf8 = Utf8.parse('12345');
            expect(utf8.value).toBe('12345');
        });

        it('should parse boolean-like strings as strings', () => {
            const utf8True = Utf8.parse('true');
            expect(utf8True.value).toBe('true');
            
            const utf8False = Utf8.parse('false');
            expect(utf8False.value).toBe('false');
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            const utf8 = Utf8.parse('⟪undefined⟫');
            expect(utf8.value).toBeUndefined();
        });

        it('should not return undefined for string containing UNDEFINED_VALUE', () => {
            const utf8 = Utf8.parse('text ⟪undefined⟫ more text');
            expect(utf8.value).toBe('text ⟪undefined⟫ more text');
        });

        it('should handle multi-line strings', () => {
            const multiline = `line 1
line 2
line 3`;
            const utf8 = Utf8.parse(multiline);
            expect(utf8.value).toBe(multiline);
        });
    });

    describe('valueOf', () => {
        it('should return the string value', () => {
            const utf8 = new Utf8('test string');
            expect(utf8.valueOf()).toBe('test string');
        });

        it('should return empty string', () => {
            const utf8 = new Utf8('');
            expect(utf8.valueOf()).toBe('');
        });

        it('should return undefined when value is undefined', () => {
            const utf8 = new Utf8(undefined);
            expect(utf8.valueOf()).toBeUndefined();
        });

        it('should return UTF-8 string', () => {
            const utf8 = new Utf8('文字列 🚀');
            expect(utf8.valueOf()).toBe('文字列 🚀');
        });
    });
});