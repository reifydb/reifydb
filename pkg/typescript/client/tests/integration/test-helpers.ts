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
     */
    static legacyCompatible(): BidirectionalSchema {
        return {
            params: Schema.optional(Schema.union(
                Schema.auto(), // Single parameter
                Schema.object({}), // Named parameters
                Schema.array(Schema.auto()) // Array parameters
            )),
            // No result schema = returns raw Value objects like before
        };
    }
}

// Default schema that mimics old behavior exactly
export const LEGACY_SCHEMA: BidirectionalSchema = TestSchemas.legacyCompatible();