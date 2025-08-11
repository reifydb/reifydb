/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { Schema, BidirectionalSchema } from "../../src";

/**
 * Helper functions for migrating existing tests to schema-only approach
 */
export class TestSchemas {
    /**
     * Create a schema for Value object parameters and results
     * This preserves the existing test behavior where Value objects go in and out
     */
    static valueObjectSchema(): BidirectionalSchema {
        return {
            params: Schema.auto(), // Accept any Value objects
            // No result schema = returns raw Value objects
        };
    }

    /**
     * Create a schema for named parameters with Value objects
     */
    static namedValueObjects(): BidirectionalSchema {
        return {
            params: Schema.object({}), // Accept any named Value object parameters
            // No result schema = returns raw Value objects
        };
    }

    /**
     * Create a schema for array parameters with Value objects
     */
    static arrayValueObjects(): BidirectionalSchema {
        return {
            params: Schema.array(Schema.auto()), // Accept array of any Value objects
            // No result schema = returns raw Value objects
        };
    }

    /**
     * Create a flexible schema that mimics the old behavior exactly
     * This schema should pass through Value objects without re-encoding them
     */
    static legacyCompatible(): BidirectionalSchema {
        return {
            params: Schema.optional(Schema.union(
                Schema.auto(), // Single parameter (Value object or primitive)
                Schema.object({}), // Named parameters (Value objects or primitives)
                Schema.array(Schema.auto()) // Array parameters (Value objects or primitives)
            )),
            // No result schema = returns raw Value objects like before
        };
    }

    /**
     * Create a schema that accepts primitives as parameters and returns primitive results
     * This schema converts Value objects to their primitive values
     */
    static primitiveResults(): BidirectionalSchema {
        return {
            params: Schema.optional(Schema.union(
                Schema.auto(), // Single parameter (Value object or primitive)
                Schema.object({}), // Named parameters (Value objects or primitives)
                Schema.array(Schema.auto()) // Array parameters (Value objects or primitives)
            )),
            result: Schema.object({}) // Use object schema to auto-convert fields to primitives
        };
    }
}

// Default schema that mimics old behavior exactly
export const LEGACY_SCHEMA: BidirectionalSchema = TestSchemas.legacyCompatible();

// Schema for tests expecting primitive results
export const PRIMITIVE_RESULT_SCHEMA: BidirectionalSchema = TestSchemas.primitiveResults();