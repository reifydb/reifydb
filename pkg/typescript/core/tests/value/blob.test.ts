// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import { describe, expect, it } from 'vitest';
import { BlobValue } from '../../src';

describe('BlobValue', () => {
    describe('constructor', () => {
        it('should create instance with Uint8Array', () => {
            const data = new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]);
            const blob = new BlobValue(data);
            expect(blob.asBytes()).toEqual(data);
            expect(blob.length()).toBe(4);
            expect(blob.isEmpty()).toBe(false);
        });

        it('should create instance with ArrayBuffer', () => {
            const buffer = new ArrayBuffer(4);
            const view = new Uint8Array(buffer);
            view[0] = 0xCA;
            view[1] = 0xFE;
            view[2] = 0xBA;
            view[3] = 0xBE;
            
            const blob = new BlobValue(buffer);
            expect(blob.asBytes()).toEqual(new Uint8Array([0xCA, 0xFE, 0xBA, 0xBE]));
        });

        it('should create instance with number array', () => {
            const data = [1, 2, 3, 255, 0];
            const blob = new BlobValue(data);
            expect(blob.asBytes()).toEqual(new Uint8Array(data));
        });

        it('should create instance with hex string', () => {
            const blob = new BlobValue('0xdeadbeef');
            expect(blob.asBytes()).toEqual(new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]));
        });

        it('should create instance with undefined', () => {
            const blob = new BlobValue(undefined);
            expect(blob.asBytes()).toBeUndefined();
            expect(blob.length()).toBe(0);
        });

        it('should throw error for invalid byte value in array', () => {
            expect(() => new BlobValue([256])).toThrow('Invalid byte value: 256');
            expect(() => new BlobValue([-1])).toThrow('Invalid byte value: -1');
            expect(() => new BlobValue([1.5])).toThrow('Invalid byte value: 1.5');
        });

        it('should throw error for invalid string', () => {
            expect(() => new BlobValue('!@#$%^&*()')).toThrow('Invalid blob string');
        });
    });

    describe('fromBytes', () => {
        it('should create blob from byte array', () => {
            const blob = BlobValue.fromBytes([0xFF, 0x00, 0xFF]);
            expect(blob.asBytes()).toEqual(new Uint8Array([0xFF, 0x00, 0xFF]));
        });
    });

    describe('fromHex', () => {
        it('should create blob from hex string with 0x prefix', () => {
            const blob = BlobValue.fromHex('0xdeadbeef');
            expect(blob.asBytes()).toEqual(new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]));
        });

        it('should create blob from hex string without prefix', () => {
            const blob = BlobValue.fromHex('cafebabe');
            expect(blob.asBytes()).toEqual(new Uint8Array([0xCA, 0xFE, 0xBA, 0xBE]));
        });

        it('should handle uppercase hex', () => {
            const blob = BlobValue.fromHex('0xDEADBEEF');
            expect(blob.asBytes()).toEqual(new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]));
        });

        it('should handle empty hex string', () => {
            const blob = BlobValue.fromHex('0x');
            expect(blob.asBytes()).toEqual(new Uint8Array([]));
            expect(blob.isEmpty()).toBe(true);
        });

        it('should throw error for odd-length hex string', () => {
            expect(() => BlobValue.fromHex('0xabc')).toThrow('Invalid hex string: odd length');
        });

        it('should throw error for invalid hex characters', () => {
            expect(() => BlobValue.fromHex('0xgg')).toThrow('Invalid hex string');
        });
    });

    describe('fromBase64', () => {
        it('should create blob from base64 string', () => {
            const blob = BlobValue.fromBase64('3q2+7w=='); // base64 for [0xDE, 0xAD, 0xBE, 0xEF]
            expect(blob.asBytes()).toEqual(new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]));
        });

        it('should handle empty base64 string', () => {
            const blob = BlobValue.fromBase64('');
            expect(blob.asBytes()).toEqual(new Uint8Array([]));
        });

        it('should throw error for invalid base64', () => {
            expect(() => BlobValue.fromBase64('!@#$')).toThrow('Invalid base64 string');
        });
    });

    describe('fromUtf8', () => {
        it('should create blob from UTF-8 string', () => {
            const blob = BlobValue.fromUtf8('Hello');
            expect(blob.asBytes()).toEqual(new Uint8Array([72, 101, 108, 108, 111]));
        });

        it('should handle Unicode characters', () => {
            const blob = BlobValue.fromUtf8('Hello 世界');
            const expected = new TextEncoder().encode('Hello 世界');
            expect(blob.asBytes()).toEqual(expected);
        });

        it('should handle empty string', () => {
            const blob = BlobValue.fromUtf8('');
            expect(blob.asBytes()).toEqual(new Uint8Array([]));
        });
    });

    describe('empty', () => {
        it('should create empty blob', () => {
            const blob = BlobValue.empty();
            expect(blob.asBytes()).toEqual(new Uint8Array([]));
            expect(blob.isEmpty()).toBe(true);
            expect(blob.length()).toBe(0);
        });
    });

    describe('toHex', () => {
        it('should convert to hex string with 0x prefix', () => {
            const blob = BlobValue.fromBytes([0xDE, 0xAD, 0xBE, 0xEF]);
            expect(blob.toHex()).toBe('0xdeadbeef');
        });

        it('should handle empty blob', () => {
            const blob = BlobValue.empty();
            expect(blob.toHex()).toBe('0x');
        });

        it('should handle single byte', () => {
            const blob = BlobValue.fromBytes([0xFF]);
            expect(blob.toHex()).toBe('0xff');
        });

        it('should pad single digit hex values', () => {
            const blob = BlobValue.fromBytes([0x0F, 0x01]);
            expect(blob.toHex()).toBe('0x0f01');
        });

        it('should return undefined for undefined blob', () => {
            const blob = new BlobValue(undefined);
            expect(blob.toHex()).toBeUndefined();
        });
    });

    describe('toBase64', () => {
        it('should convert to base64 string', () => {
            const blob = BlobValue.fromBytes([0xDE, 0xAD, 0xBE, 0xEF]);
            expect(blob.toBase64()).toBe('3q2+7w==');
        });

        it('should handle empty blob', () => {
            const blob = BlobValue.empty();
            expect(blob.toBase64()).toBe('');
        });

        it('should return undefined for undefined blob', () => {
            const blob = new BlobValue(undefined);
            expect(blob.toBase64()).toBeUndefined();
        });
    });

    describe('toUtf8', () => {
        it('should convert to UTF-8 string', () => {
            const blob = BlobValue.fromBytes([72, 101, 108, 108, 111]);
            expect(blob.toUtf8()).toBe('Hello');
        });

        it('should handle Unicode bytes', () => {
            const original = 'Hello 世界';
            const blob = BlobValue.fromUtf8(original);
            expect(blob.toUtf8()).toBe(original);
        });

        it('should handle empty blob', () => {
            const blob = BlobValue.empty();
            expect(blob.toUtf8()).toBe('');
        });

        it('should return undefined for undefined blob', () => {
            const blob = new BlobValue(undefined);
            expect(blob.toUtf8()).toBeUndefined();
        });
    });

    describe('toString', () => {
        it('should format as hex string', () => {
            const blob = BlobValue.fromBytes([0xDE, 0xAD, 0xBE, 0xEF]);
            expect(blob.toString()).toBe('0xdeadbeef');
        });

        it('should format empty blob', () => {
            const blob = BlobValue.empty();
            expect(blob.toString()).toBe('0x');
        });

        it('should format undefined blob', () => {
            const blob = new BlobValue(undefined);
            expect(blob.toString()).toBe('undefined');
        });
    });

    describe('parse', () => {
        it('should parse hex string with 0x prefix', () => {
            const blob = BlobValue.parse('0xdeadbeef');
            expect(blob.asBytes()).toEqual(new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]));
        });

        it('should parse hex string without prefix', () => {
            const blob = BlobValue.parse('cafebabe');
            expect(blob.asBytes()).toEqual(new Uint8Array([0xCA, 0xFE, 0xBA, 0xBE]));
        });

        it('should parse with whitespace', () => {
            const blob = BlobValue.parse('  0xdeadbeef  ');
            expect(blob.asBytes()).toEqual(new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]));
        });

        it('should return undefined for empty string', () => {
            expect(BlobValue.parse('').value).toBeUndefined();
            expect(BlobValue.parse('   ').value).toBeUndefined();
        });

        it('should return undefined for UNDEFINED_VALUE', () => {
            expect(BlobValue.parse('⟪undefined⟫').value).toBeUndefined();
        });

        it('should throw error for invalid string', () => {
            expect(() => BlobValue.parse('not-hex')).toThrow('Cannot parse');
            expect(() => BlobValue.parse('3q2+7w==')).toThrow('Cannot parse'); // base64 is not supported
        });
    });

    describe('equals', () => {
        it('should compare equal blobs', () => {
            const blob1 = BlobValue.fromBytes([1, 2, 3]);
            const blob2 = BlobValue.fromBytes([1, 2, 3]);
            expect(blob1.equals(blob2)).toBe(true);
        });

        it('should compare different blobs', () => {
            const blob1 = BlobValue.fromBytes([1, 2, 3]);
            const blob2 = BlobValue.fromBytes([1, 2, 4]);
            expect(blob1.equals(blob2)).toBe(false);
        });

        it('should compare different length blobs', () => {
            const blob1 = BlobValue.fromBytes([1, 2, 3]);
            const blob2 = BlobValue.fromBytes([1, 2]);
            expect(blob1.equals(blob2)).toBe(false);
        });

        it('should compare empty blobs', () => {
            const blob1 = BlobValue.empty();
            const blob2 = BlobValue.empty();
            expect(blob1.equals(blob2)).toBe(true);
        });

        it('should compare undefined blobs', () => {
            const blob1 = new BlobValue(undefined);
            const blob2 = new BlobValue(undefined);
            expect(blob1.equals(blob2)).toBe(true);
        });

        it('should compare undefined with defined blob', () => {
            const blob1 = new BlobValue(undefined);
            const blob2 = BlobValue.fromBytes([1, 2, 3]);
            expect(blob1.equals(blob2)).toBe(false);
        });
    });

    describe('valueOf', () => {
        it('should return the byte array', () => {
            const data = new Uint8Array([1, 2, 3]);
            const blob = new BlobValue(data);
            expect(blob.valueOf()).toEqual(data);
        });

        it('should return undefined when value is undefined', () => {
            const blob = new BlobValue(undefined);
            expect(blob.valueOf()).toBeUndefined();
        });

        it('should return a copy of the bytes', () => {
            const data = new Uint8Array([1, 2, 3]);
            const blob = new BlobValue(data);
            const bytes = blob.valueOf()!;
            bytes[0] = 99;
            // Original should not be modified
            expect(blob.asBytes()![0]).toBe(1);
        });
    });

    describe('round-trip conversions', () => {
        it('should round-trip through hex', () => {
            const original = new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]);
            const blob = new BlobValue(original);
            const hex = blob.toHex()!;
            const reconstructed = BlobValue.fromHex(hex);
            expect(reconstructed.asBytes()).toEqual(original);
        });

        it('should round-trip through base64', () => {
            const original = new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]);
            const blob = new BlobValue(original);
            const base64 = blob.toBase64()!;
            const reconstructed = BlobValue.fromBase64(base64);
            expect(reconstructed.asBytes()).toEqual(original);
        });

        it('should round-trip through UTF-8', () => {
            const original = 'Hello, 世界! 🌍';
            const blob = BlobValue.fromUtf8(original);
            const utf8 = blob.toUtf8()!;
            expect(utf8).toBe(original);
        });
    });
});