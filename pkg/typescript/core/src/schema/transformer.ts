/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import type { SchemaNode, ValueType } from './types';
import { TypeValuePair } from '../value/type';
import {
    Value,
    BoolValue,
    Int1Value,
    Int2Value,
    Int4Value,
    Int8Value,
    Int16Value,
    Uint1Value,
    Uint2Value,
    Uint4Value,
    Uint8Value,
    Uint16Value,
    Float4Value,
    Float8Value,
    Utf8Value,
    DateValue,
    DateTimeValue,
    TimeValue,
    IntervalValue,
    Uuid4Value,
    Uuid7Value,
    UndefinedValue,
    BlobValue,
    RowIdValue
} from '../value';

/**
 * Handles transformation between primitives and Value objects according to schemas
 */
export class SchemaTransformer {
    /**
     * Map of Value type names to their constructors
     */
    private static readonly VALUE_CONSTRUCTORS: Record<ValueType, any> = {
        'BoolValue': BoolValue,
        'Int1Value': Int1Value,
        'Int2Value': Int2Value,
        'Int4Value': Int4Value,
        'Int8Value': Int8Value,
        'Int16Value': Int16Value,
        'Uint1Value': Uint1Value,
        'Uint2Value': Uint2Value,
        'Uint4Value': Uint4Value,
        'Uint8Value': Uint8Value,
        'Uint16Value': Uint16Value,
        'Float4Value': Float4Value,
        'Float8Value': Float8Value,
        'Utf8Value': Utf8Value,
        'DateValue': DateValue,
        'DateTimeValue': DateTimeValue,
        'TimeValue': TimeValue,
        'IntervalValue': IntervalValue,
        'Uuid4Value': Uuid4Value,
        'Uuid7Value': Uuid7Value,
        'UndefinedValue': UndefinedValue,
        'BlobValue': BlobValue,
        'RowIdValue': RowIdValue
    };

    /**
     * Check if a value is a Value object
     */
    private static isValueObject(value: any): value is Value {
        return value && typeof value === 'object' && 
               'encode' in value && typeof value.encode === 'function' &&
               'type' in value;
    }

    /**
     * Extract the primitive value from a Value object
     */
    private static extractValue(valueObj: any): any {
        // Most Value objects have a .value property
        if ('value' in valueObj) {
            return valueObj.value;
        }
        // Fallback to undefined for special cases
        return undefined;
    }

    /**
     * Check if a Value object matches a specific type
     */
    private static isValueType(value: any, type: ValueType): boolean {
        if (!this.isValueObject(value)) return false;
        const Constructor = this.VALUE_CONSTRUCTORS[type];
        return value instanceof Constructor;
    }

    /**
     * Convert a primitive value to the appropriate Value object
     */
    private static primitiveToValue(value: any, targetType?: string): Value {
        if (value === null || value === undefined) {
            return new UndefinedValue();
        }

        switch (typeof value) {
            case 'boolean':
                return new BoolValue(value);
            
            case 'number':
                if (Number.isInteger(value)) {
                    // Choose appropriate integer type based on range
                    if (value >= -128 && value <= 127) {
                        return new Int1Value(value);
                    } else if (value >= -32768 && value <= 32767) {
                        return new Int2Value(value);
                    } else if (value >= -2147483648 && value <= 2147483647) {
                        return new Int4Value(value);
                    } else {
                        return new Int8Value(value);
                    }
                } else {
                    // Float
                    return new Float8Value(value);
                }
            
            case 'string':
                return new Utf8Value(value);
            
            case 'bigint':
                return new Int8Value(value);
            
            default:
                if (value instanceof Date) {
                    return new DateTimeValue(value);
                }
                throw new Error(`Cannot convert primitive type ${typeof value} to Value object`);
        }
    }

    /**
     * Smart auto-detection and conversion
     */
    private static autoEncode(value: any, hint?: string): Value | TypeValuePair {
        // If already a Value object, return as-is
        if (this.isValueObject(value)) {
            return value;
        }

        // Use hint if provided
        if (hint === 'integer' && typeof value === 'number') {
            return this.primitiveToValue(Math.floor(value));
        } else if (hint === 'float' && typeof value === 'number') {
            return new Float8Value(value);
        } else if (hint) {
            // Hint is a primitive type
            return this.primitiveToValue(value, hint);
        }

        // Auto-detect
        return this.primitiveToValue(value);
    }

    /**
     * Encode parameters according to schema (primitives → Value objects)
     */
    static encodeParams(params: any, schema: SchemaNode): any {
        switch (schema.kind) {
            case 'primitive':
                // Convert primitive to appropriate Value object
                return this.primitiveToValue(params, schema.type);
            
            case 'value':
                // Expect Value object, validate type
                if (!this.isValueType(params, schema.type)) {
                    // Try to convert if it's a primitive
                    if (!this.isValueObject(params)) {
                        const Constructor = this.VALUE_CONSTRUCTORS[schema.type];
                        try {
                            // Special handling for different Value types
                            switch (schema.type) {
                                case 'BoolValue':
                                    return new BoolValue(Boolean(params));
                                case 'Utf8Value':
                                    return new Utf8Value(String(params));
                                case 'DateTimeValue':
                                    return new DateTimeValue(params instanceof Date ? params : new Date(params));
                                case 'DateValue':
                                    return new DateValue(params instanceof Date ? params : new Date(params));
                                case 'TimeValue':
                                    return new TimeValue(params);
                                case 'IntervalValue':
                                    return new IntervalValue(params);
                                case 'UndefinedValue':
                                    return new UndefinedValue();
                                default:
                                    // For numeric types
                                    if (schema.type.includes('Int') || schema.type.includes('Uint') || schema.type.includes('Float')) {
                                        return new Constructor(params);
                                    }
                                    throw new Error(`Cannot convert to ${schema.type}`);
                            }
                        } catch (e) {
                            throw new Error(`Expected ${schema.type}, got ${typeof params}: ${e}`);
                        }
                    }
                    throw new Error(`Expected ${schema.type}, got ${params.constructor.name}`);
                }
                return params;
            
            case 'auto':
                // Auto-detect and convert
                return this.autoEncode(params, schema.hint);
            
            case 'object':
                if (!params || typeof params !== 'object') {
                    throw new Error(`Expected object, got ${typeof params}`);
                }
                const encoded: any = {};
                for (const [key, propSchema] of Object.entries(schema.properties)) {
                    if (key in params) {
                        encoded[key] = this.encodeParams(params[key], propSchema);
                    }
                }
                return encoded;
            
            case 'array':
                if (!Array.isArray(params)) {
                    throw new Error(`Expected array, got ${typeof params}`);
                }
                return params.map((item: any) => this.encodeParams(item, schema.items));
            
            case 'tuple':
                if (!Array.isArray(params)) {
                    throw new Error(`Expected tuple array, got ${typeof params}`);
                }
                if (params.length !== schema.items.length) {
                    throw new Error(`Expected tuple of length ${schema.items.length}, got ${params.length}`);
                }
                return params.map((item: any, index: number) => 
                    this.encodeParams(item, schema.items[index])
                );
            
            case 'optional':
                return params == null ? undefined : this.encodeParams(params, schema.schema);
            
            case 'union':
                // Try each schema until one works
                for (const unionSchema of schema.types) {
                    try {
                        return this.encodeParams(params, unionSchema);
                    } catch {
                        // Try next schema
                    }
                }
                throw new Error(`No matching union type for value: ${JSON.stringify(params)}`);
            
            default:
                throw new Error(`Unknown schema kind: ${(schema as any).kind}`);
        }
    }

    /**
     * Decode results according to schema (Value objects → primitives)
     */
    static decodeResult(result: any, schema: SchemaNode): any {
        switch (schema.kind) {
            case 'primitive':
                // Extract .value from Value object
                if (this.isValueObject(result)) {
                    const value = this.extractValue(result);
                    // Handle type conversions
                    switch (schema.type) {
                        case 'string':
                            return String(value);
                        case 'number':
                            return typeof value === 'bigint' ? Number(value) : value;
                        case 'boolean':
                            return Boolean(value);
                        case 'bigint':
                            return typeof value === 'bigint' ? value : BigInt(value);
                        case 'Date':
                            return value instanceof Date ? value : new Date(value);
                        case 'undefined':
                        case 'null':
                            return value ?? undefined;
                        default:
                            return value;
                    }
                }
                return result;
            
            case 'value':
                // Keep as Value object
                if (!this.isValueObject(result)) {
                    throw new Error(`Expected Value object, got ${typeof result}`);
                }
                return result;
            
            case 'auto':
                // Return as-is
                return result;
            
            case 'object':
                if (!result || typeof result !== 'object') {
                    return result;
                }
                const decoded: any = {};
                for (const [key, propSchema] of Object.entries(schema.properties)) {
                    if (key in result) {
                        decoded[key] = this.decodeResult(result[key], propSchema);
                    }
                }
                return decoded;
            
            case 'array':
                if (!Array.isArray(result)) {
                    return result;
                }
                return result.map((item: any) => this.decodeResult(item, schema.items));
            
            case 'tuple':
                if (!Array.isArray(result)) {
                    return result;
                }
                return result.map((item: any, index: number) => 
                    index < schema.items.length 
                        ? this.decodeResult(item, schema.items[index])
                        : item
                );
            
            case 'optional':
                return result == null ? undefined : this.decodeResult(result, schema.schema);
            
            case 'union':
                // Try to match and decode with appropriate schema
                for (const unionSchema of schema.types) {
                    if (this.matchesSchema(result, unionSchema)) {
                        return this.decodeResult(result, unionSchema);
                    }
                }
                // Fallback to first schema
                return this.decodeResult(result, schema.types[0]);
            
            default:
                return result;
        }
    }

    /**
     * Check if a value matches a schema (for union type detection)
     */
    private static matchesSchema(value: any, schema: SchemaNode): boolean {
        switch (schema.kind) {
            case 'primitive':
                if (this.isValueObject(value)) {
                    // Check if Value object can be decoded to this primitive
                    const primitiveValue = this.extractValue(value);
                    switch (schema.type) {
                        case 'string':
                            return typeof primitiveValue === 'string';
                        case 'number':
                            return typeof primitiveValue === 'number' || typeof primitiveValue === 'bigint';
                        case 'boolean':
                            return typeof primitiveValue === 'boolean';
                        case 'bigint':
                            return typeof primitiveValue === 'bigint';
                        case 'Date':
                            return primitiveValue instanceof Date;
                        case 'undefined':
                        case 'null':
                            return primitiveValue == null;
                    }
                }
                return typeof value === schema.type;
            
            case 'value':
                return this.isValueType(value, schema.type);
            
            case 'object':
                return value && typeof value === 'object' && !Array.isArray(value);
            
            case 'array':
                return Array.isArray(value);
            
            case 'tuple':
                return Array.isArray(value) && value.length === schema.items.length;
            
            case 'optional':
                return value == null || this.matchesSchema(value, schema.schema);
            
            case 'auto':
                return true; // Auto matches anything
            
            case 'union':
                return schema.types.some(t => this.matchesSchema(value, t));
            
            default:
                return false;
        }
    }

    /**
     * Validate a value against a schema
     */
    static validate(value: any, schema: SchemaNode): { valid: boolean; errors?: string[] } {
        const errors: string[] = [];
        
        try {
            // Try to encode/decode to validate structure
            if (schema.kind === 'primitive' || schema.kind === 'value') {
                this.encodeParams(value, schema);
            } else {
                this.validateRecursive(value, schema, '', errors);
            }
            
            return { valid: errors.length === 0, errors: errors.length > 0 ? errors : undefined };
        } catch (e) {
            errors.push(String(e));
            return { valid: false, errors };
        }
    }

    private static validateRecursive(value: any, schema: SchemaNode, path: string, errors: string[]): void {
        switch (schema.kind) {
            case 'object':
                if (!value || typeof value !== 'object') {
                    errors.push(`${path}: Expected object, got ${typeof value}`);
                    return;
                }
                for (const [key, propSchema] of Object.entries(schema.properties)) {
                    const propPath = path ? `${path}.${key}` : key;
                    if (!(key in value) && propSchema.kind !== 'optional') {
                        errors.push(`${propPath}: Required property missing`);
                    } else if (key in value) {
                        this.validateRecursive(value[key], propSchema, propPath, errors);
                    }
                }
                break;
            
            case 'array':
                if (!Array.isArray(value)) {
                    errors.push(`${path}: Expected array, got ${typeof value}`);
                    return;
                }
                value.forEach((item, index) => {
                    this.validateRecursive(item, schema.items, `${path}[${index}]`, errors);
                });
                break;
            
            case 'optional':
                if (value != null) {
                    this.validateRecursive(value, schema.schema, path, errors);
                }
                break;
            
            // Add more validation as needed
        }
    }
}