/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { Schema, BidirectionalSchema } from "@reifydb/core";

/**
 * Helper functions to create common schemas for easier migration
 */
export class SchemaHelpers {
    /**
     * Create a schema that accepts any parameters and returns raw Value objects
     */
    static anySchema(): BidirectionalSchema {
        return {
            params: Schema.optional(Schema.auto()),
            // No result schema - returns Value objects
        };
    }

    /**
     * Create a schema for named parameters with auto-detection
     */
    static namedParams(): BidirectionalSchema {
        return {
            params: Schema.object({}), // Empty object schema allows any properties
        };
    }

    /**
     * Create a schema for array parameters with auto-detection  
     */
    static arrayParams(): BidirectionalSchema {
        return {
            params: Schema.array(Schema.auto()),
        };
    }

    /**
     * Create a schema that returns primitive values for common types
     */
    static primitiveResults(): BidirectionalSchema {
        return {
            result: Schema.auto() // Auto-detect and return primitives
        };
    }

    /**
     * Create a flexible schema for testing that accepts anything
     */
    static flexibleSchema(): BidirectionalSchema {
        return {
            params: Schema.auto(),
            result: Schema.auto()
        };
    }
}

// Legacy compatibility - create a default flexible schema 
export const DEFAULT_SCHEMA: BidirectionalSchema = SchemaHelpers.flexibleSchema();