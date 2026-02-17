// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
import {describe, expect, it} from 'vitest';
import {IdentityIdValue} from '../../src';
import { validate, version, NIL as NIL_UUID } from 'uuid';

describe('IdentityIdValue', () => {
    describe('constructor', () => {
        it('should create instance with valid UUID v7', () => {
            const uuid = IdentityIdValue.generate().value!;
            const identityId = new IdentityIdValue(uuid);
            expect(identityId.value).toBe(uuid.toLowerCase());
            expect(identityId.type).toBe('IdentityId');
        });

        it('should create instance with nil UUID', () => {
            const identityId = new IdentityIdValue(NIL_UUID);
            expect(identityId.value).toBe(NIL_UUID);
            expect(identityId.type).toBe('IdentityId');
        });

        it('should create instance with undefined value', () => {
            const identityId = new IdentityIdValue(undefined);
            expect(identityId.value).toBeUndefined();
            expect(identityId.type).toBe('IdentityId');
        });

        it('should create instance with no arguments', () => {
            const identityId = new IdentityIdValue();
            expect(identityId.value).toBeUndefined();
            expect(identityId.type).toBe('IdentityId');
        });

        it('should convert UUID to lowercase', () => {
            const upperCaseUuid = IdentityIdValue.generate().value!.toUpperCase();
            const identityId = new IdentityIdValue(upperCaseUuid);
            expect(identityId.value).toBe(upperCaseUuid.toLowerCase());
        });

        it('should throw error for non-string value', () => {
            expect(() => new IdentityIdValue(123 as any)).toThrow('IdentityId value must be a string');
        });

        it('should throw error for invalid UUID format', () => {
            expect(() => new IdentityIdValue('not-a-uuid')).toThrow('Invalid UUID format');
        });

        it('should throw error for UUID v4', () => {
            const uuidV4 = '550e8400-e29b-41d4-a716-446655440000';
            expect(() => new IdentityIdValue(uuidV4)).toThrow('Invalid UUID version for IdentityId: expected v7, got v4');
        });
    });

    describe('static methods', () => {
        describe('generate', () => {
            it('should generate a new UUID v7', () => {
                const identityId = IdentityIdValue.generate();
                expect(identityId.value).toBeDefined();
                expect(validate(identityId.value!)).toBe(true);
                expect(version(identityId.value!)).toBe(7);
            });

            it('should generate unique values', () => {
                const id1 = IdentityIdValue.generate();
                const id2 = IdentityIdValue.generate();
                expect(id1.value).not.toBe(id2.value);
            });
        });

        describe('nil', () => {
            it('should return nil UUID', () => {
                const identityId = IdentityIdValue.nil();
                expect(identityId.value).toBe(NIL_UUID);
                expect(identityId.isNil()).toBe(true);
            });
        });

        describe('parse', () => {
            it('should parse valid UUID v7 string', () => {
                const uuid = IdentityIdValue.generate().value!;
                const identityId = IdentityIdValue.parse(uuid);
                expect(identityId.value).toBe(uuid);
            });

            it('should parse nil UUID', () => {
                const identityId = IdentityIdValue.parse(NIL_UUID);
                expect(identityId.value).toBe(NIL_UUID);
            });

            it('should trim whitespace', () => {
                const uuid = IdentityIdValue.generate().value!;
                const identityId = IdentityIdValue.parse(`  ${uuid}  `);
                expect(identityId.value).toBe(uuid);
            });

            it('should return undefined for empty string', () => {
                const identityId = IdentityIdValue.parse('');
                expect(identityId.value).toBeUndefined();
            });

            it('should return undefined for whitespace-only string', () => {
                const identityId = IdentityIdValue.parse('   ');
                expect(identityId.value).toBeUndefined();
            });

            it('should return undefined for NONE_VALUE', () => {
                const identityId = IdentityIdValue.parse('⟪none⟫');
                expect(identityId.value).toBeUndefined();
            });

            it('should throw error for invalid UUID format', () => {
                expect(() => IdentityIdValue.parse('not-a-uuid')).toThrow('Cannot parse "not-a-uuid" as IdentityId');
            });

            it('should throw error for UUID v4', () => {
                const uuidV4 = '550e8400-e29b-41d4-a716-446655440000';
                expect(() => IdentityIdValue.parse(uuidV4)).toThrow('Cannot parse "550e8400-e29b-41d4-a716-446655440000" as IdentityId: wrong UUID version');
            });
        });
    });

    describe('instance methods', () => {
        describe('valueOf', () => {
            it('should return the UUID string value', () => {
                const uuid = IdentityIdValue.generate().value!;
                const identityId = new IdentityIdValue(uuid);
                expect(identityId.valueOf()).toBe(uuid);
            });

            it('should return undefined when value is undefined', () => {
                const identityId = new IdentityIdValue(undefined);
                expect(identityId.valueOf()).toBeUndefined();
            });
        });

        describe('toString', () => {
            it('should return the UUID string', () => {
                const uuid = IdentityIdValue.generate().value!;
                const identityId = new IdentityIdValue(uuid);
                expect(identityId.toString()).toBe(uuid);
            });

            it('should return "none" when value is undefined', () => {
                const identityId = new IdentityIdValue(undefined);
                expect(identityId.toString()).toBe('none');
            });
        });

        describe('getTimestamp', () => {
            it('should extract timestamp from UUID v7', () => {
                const identityId = IdentityIdValue.generate();
                const timestamp = identityId.getTimestamp();
                expect(timestamp).toBeDefined();
                expect(typeof timestamp).toBe('number');
                // Check that timestamp is reasonable (within last minute)
                const now = Date.now();
                expect(timestamp!).toBeLessThanOrEqual(now);
                expect(timestamp!).toBeGreaterThan(now - 60000);
            });

            it('should return undefined for nil UUID', () => {
                const identityId = IdentityIdValue.nil();
                expect(identityId.getTimestamp()).toBeUndefined();
            });

            it('should return undefined for undefined value', () => {
                const identityId = new IdentityIdValue(undefined);
                expect(identityId.getTimestamp()).toBeUndefined();
            });
        });

        describe('isNil', () => {
            it('should return true for nil UUID', () => {
                const identityId = IdentityIdValue.nil();
                expect(identityId.isNil()).toBe(true);
            });

            it('should return false for generated UUID', () => {
                const identityId = IdentityIdValue.generate();
                expect(identityId.isNil()).toBe(false);
            });

            it('should return false for undefined value', () => {
                const identityId = new IdentityIdValue(undefined);
                expect(identityId.isNil()).toBe(false);
            });
        });

        describe('equals', () => {
            it('should return true for equal IdentityId values', () => {
                const uuid = IdentityIdValue.generate().value!;
                const id1 = new IdentityIdValue(uuid);
                const id2 = new IdentityIdValue(uuid);
                expect(id1.equals(id2)).toBe(true);
            });

            it('should return false for different IdentityId values', () => {
                const id1 = IdentityIdValue.generate();
                const id2 = IdentityIdValue.generate();
                expect(id1.equals(id2)).toBe(false);
            });

            it('should return true for both undefined values', () => {
                const id1 = new IdentityIdValue(undefined);
                const id2 = new IdentityIdValue(undefined);
                expect(id1.equals(id2)).toBe(true);
            });

            it('should return false for different types', () => {
                const identityId = IdentityIdValue.generate();
                const otherValue = { type: 'Uuid7', value: identityId.value } as any;
                expect(identityId.equals(otherValue)).toBe(false);
            });
        });

        describe('encode', () => {
            it('should encode IdentityId with value', () => {
                const uuid = IdentityIdValue.generate().value!;
                const identityId = new IdentityIdValue(uuid);
                const encoded = identityId.encode();
                expect(encoded.type).toBe('IdentityId');
                expect(encoded.value).toBe(uuid);
            });

            it('should encode undefined value', () => {
                const identityId = new IdentityIdValue(undefined);
                const encoded = identityId.encode();
                expect(encoded.type).toBe('IdentityId');
                expect(encoded.value).toBe('⟪none⟫');
            });
        });
    });
});