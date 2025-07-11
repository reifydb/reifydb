/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {describe, expect, it} from 'vitest';
import {decodeValue} from '../../src/decoder';
import {DataType} from "../../src";

const UNDEFINED_VALUE = "⟪undefined⟫";

describe('decodeValue', () => {
    describe('undefined value handling', () => {
        it('should return undefined for ⟪undefined⟫ regardless of type', () => {
            const data_types: DataType[] = ['Bool', 'Float4', 'Int1', 'Utf8', 'Undefined'];

            data_types.forEach(data_type => {
                expect(decodeValue(data_type, UNDEFINED_VALUE)).toBeUndefined();
            });
        });

        it('should return undefined for exact UNDEFINED_VALUE string match', () => {
            expect(decodeValue('Bool', UNDEFINED_VALUE)).toBeUndefined();
        });
    });

    describe('Bool type', () => {
        it('should return true for "true" string', () => {
            expect(decodeValue('Bool', 'true')).toBe(true);
        });

        it('should return false for "false" string', () => {
            expect(decodeValue('Bool', 'false')).toBe(false);
        });

    });

    describe('Float types (Float4, Float8)', () => {
        const floatKinds: DataType[] = ['Float4', 'Float8'];

        floatKinds.forEach(data_type => {
            describe(`${data_type}`, () => {
                it('should convert valid float strings to numbers', () => {
                    expect(decodeValue(data_type, '3.14')).toBe(3.14);
                    expect(decodeValue(data_type, '-2.5')).toBe(-2.5);
                    expect(decodeValue(data_type, '0.0')).toBe(0.0);
                    expect(decodeValue(data_type, '123.456')).toBe(123.456);
                });

                it('should convert integer strings to numbers', () => {
                    expect(decodeValue(data_type, '42')).toBe(42);
                    expect(decodeValue(data_type, '-17')).toBe(-17);
                    expect(decodeValue(data_type, '0')).toBe(0);
                });

                it('should handle scientific notation', () => {
                    expect(decodeValue(data_type, '1e5')).toBe(100000);
                    expect(decodeValue(data_type, '2.5e-3')).toBe(0.0025);
                    expect(decodeValue(data_type, '-1.23e4')).toBe(-12300);
                });
            });
        });
    });

    describe('Small singed integer types (Int1, Int2, Int4)', () => {
        const intKinds: DataType[] = ['Int1', 'Int2', 'Int4'];

        intKinds.forEach(data_type => {
            describe(`${data_type}`, () => {
                it('should convert valid integer strings to numbers', () => {
                    expect(decodeValue(data_type, '42')).toBe(42);
                    expect(decodeValue(data_type, '-17')).toBe(-17);
                    expect(decodeValue(data_type, '0')).toBe(0);
                    expect(decodeValue(data_type, '123')).toBe(123);
                });

                it('should handle edge cases', () => {
                    expect(decodeValue(data_type, '2147483647')).toBe(2147483647);
                    expect(decodeValue(data_type, '-2147483648')).toBe(-2147483648);
                });

            });
        });
    });

    describe('Small unsinged integer types (Uint1, Uint2, Uint4)', () => {
        const intKinds: DataType[] = ['Uint1', 'Uint2', 'Uint4'];

        intKinds.forEach(data_type => {
            describe(`${data_type}`, () => {
                it('should convert valid integer strings to numbers', () => {
                    expect(decodeValue(data_type, '42')).toBe(42);
                    expect(decodeValue(data_type, '0')).toBe(0);
                    expect(decodeValue(data_type, '123')).toBe(123);
                });

                it('should handle edge cases', () => {
                    expect(decodeValue(data_type, '2147483647')).toBe(2147483647);
                });

            });
        });
    });


    describe('big signed integer (Int8, Int16)', () => {
        const bigintKinds: DataType[] = ['Int8', 'Int16'];

        bigintKinds.forEach(data_type => {
            describe(`${data_type}`, () => {
                it('should convert valid integer strings to BigInt', () => {
                    expect(decodeValue(data_type, '42')).toBe(BigInt(42));
                    expect(decodeValue(data_type, '-17')).toBe(BigInt(-17));
                    expect(decodeValue(data_type, '0')).toBe(BigInt(0));
                    expect(decodeValue(data_type, '123')).toBe(BigInt(123));
                });

                it('should handle large numbers', () => {
                    expect(decodeValue(data_type, '9223372036854775807')).toBe(BigInt('9223372036854775807'));
                    expect(decodeValue(data_type, '-9223372036854775808')).toBe(BigInt('-9223372036854775808'));
                });

                it('should handle very large numbers', () => {
                    const largeNumber = '123456789012345678901234567890';
                    expect(decodeValue(data_type, largeNumber)).toBe(BigInt(largeNumber));
                });
            });
        });
    });

    describe('big unsigned integer(Uint8, Uint16)', () => {
        const bigintKinds: DataType[] = ['Uint8', 'Uint16'];

        bigintKinds.forEach(data_type => {
            describe(`${data_type}`, () => {
                it('should convert valid integer strings to BigInt', () => {
                    expect(decodeValue(data_type, '42')).toBe(BigInt(42));
                    expect(decodeValue(data_type, '0')).toBe(BigInt(0));
                    expect(decodeValue(data_type, '123')).toBe(BigInt(123));
                });

                it('should handle large numbers', () => {
                    expect(decodeValue(data_type, '18446744073709551615')).toBe(BigInt('18446744073709551615'));
                });

                it('should handle very large numbers', () => {
                    const largeNumber = '123456789012345678901234567890';
                    expect(decodeValue(data_type, largeNumber)).toBe(BigInt(largeNumber));
                });
            });
        });
    });

    describe('Utf8 type', () => {
        it('should return the string value as-is', () => {
            expect(decodeValue('Utf8', 'hello world')).toBe('hello world');
            expect(decodeValue('Utf8', '')).toBe('');
            expect(decodeValue('Utf8', '123')).toBe('123');
            expect(decodeValue('Utf8', 'true')).toBe('true');
            expect(decodeValue('Utf8', 'special chars: 🚀 ñ ü')).toBe('special chars: 🚀 ñ ü');
        });

        it('should handle whitespace and special characters', () => {
            expect(decodeValue('Utf8', '  spaces  ')).toBe('  spaces  ');
            expect(decodeValue('Utf8', '\n\t\r')).toBe('\n\t\r');
            expect(decodeValue('Utf8', 'line1\nline2')).toBe('line1\nline2');
        });

        it('should handle unicode characters', () => {
            expect(decodeValue('Utf8', '👨‍💻🌟')).toBe('👨‍💻🌟');
            expect(decodeValue('Utf8', 'café')).toBe('café');
            expect(decodeValue('Utf8', '中文')).toBe('中文');
        });
    });

    describe('Undefined type', () => {
        it('should always return undefined regardless of value', () => {
            expect(decodeValue('Undefined', 'anything')).toBeUndefined();
            expect(decodeValue('Undefined', 'true')).toBeUndefined();
            expect(decodeValue('Undefined', '123')).toBeUndefined();
            expect(decodeValue('Undefined', '')).toBeUndefined();
            expect(decodeValue('Undefined', UNDEFINED_VALUE)).toBeUndefined();
        });
    });

    describe('unknown type', () => {
        it('should throw an error for unknown data_types', () => {
            // @ts-expect-error - Testing invalid data_type
            expect(() => decodeValue('InvalidKind', 'value')).toThrow('Unknown data type: InvalidKind');

            // @ts-expect-error - Testing invalid data_type
            expect(() => decodeValue('String', 'value')).toThrow('Unknown data type: String');

            // @ts-expect-error - Testing invalid data_type
            expect(() => decodeValue('', 'value')).toThrow('Unknown data type: ');
        });
    });
});