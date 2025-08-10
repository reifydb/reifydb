/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { describe, expect, it } from 'vitest';
import { decode } from '../src/decoder';
import { BoolValue, Int4Value, Utf8Value, UndefinedValue } from '../src/value';

describe('decode', () => {
    it('should decode Bool type with "true" value', () => {
        const pair = { type: 'Bool' as const, value: 'true' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(BoolValue);
        expect(result.type).toBe('Bool');
        expect(result.valueOf()).toBe(true);
    });

    it('should decode Bool type with "false" value', () => {
        const pair = { type: 'Bool' as const, value: 'false' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(BoolValue);
        expect(result.type).toBe('Bool');
        expect(result.valueOf()).toBe(false);
    });

    it('should decode Bool type with empty value', () => {
        const pair = { type: 'Bool' as const, value: '' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(BoolValue);
        expect(result.type).toBe('Bool');
        expect(result.valueOf()).toBeUndefined();
    });

    it('should decode Int4 type with positive number', () => {
        const pair = { type: 'Int4' as const, value: '42' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(Int4Value);
        expect(result.type).toBe('Int4');
        expect(result.valueOf()).toBe(42);
    });

    it('should decode Int4 type with negative number', () => {
        const pair = { type: 'Int4' as const, value: '-123' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(Int4Value);
        expect(result.type).toBe('Int4');
        expect(result.valueOf()).toBe(-123);
    });

    it('should decode Int4 type with empty value', () => {
        const pair = { type: 'Int4' as const, value: '' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(Int4Value);
        expect(result.type).toBe('Int4');
        expect(result.valueOf()).toBeUndefined();
    });

    it('should decode Utf8 type with string value', () => {
        const pair = { type: 'Utf8' as const, value: 'hello world' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(Utf8Value);
        expect(result.type).toBe('Utf8');
        expect(result.valueOf()).toBe('hello world');
    });

    it('should decode Utf8 type with empty string value', () => {
        const pair = { type: 'Utf8' as const, value: '' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(Utf8Value);
        expect(result.type).toBe('Utf8');
        expect(result.valueOf()).toBe('');
    });

    it('should decode Undefined type', () => {
        const pair = { type: 'Undefined' as const, value: '' };
        const result = decode(pair);
        
        expect(result).toBeInstanceOf(UndefinedValue);
        expect(result.type).toBe('Undefined');
        expect(result.valueOf()).toBeUndefined();
    });

    it('should throw error for unsupported type', () => {
        const pair = { type: 'InvalidType' as any, value: 'test' };
        
        expect(() => decode(pair)).toThrow('Unsupported type: InvalidType');
    });

    it('should handle round-trip encoding/decoding for Bool', () => {
        const original = new BoolValue(true);
        const encoded = original.encode();
        const decoded = decode(encoded);
        
        expect(decoded.type).toBe(original.type);
        expect(decoded.valueOf()).toBe(original.valueOf());
    });

    it('should handle round-trip encoding/decoding for Int4', () => {
        const original = new Int4Value(42);
        const encoded = original.encode();
        const decoded = decode(encoded);
        
        expect(decoded.type).toBe(original.type);
        expect(decoded.valueOf()).toBe(original.valueOf());
    });

    it('should handle round-trip encoding/decoding for Utf8', () => {
        const original = new Utf8Value('hello world');
        const encoded = original.encode();
        const decoded = decode(encoded);
        
        expect(decoded.type).toBe(original.type);
        expect(decoded.valueOf()).toBe(original.valueOf());
    });
});