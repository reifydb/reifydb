/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
import { BidirectionalSchema } from "@reifydb/core";
/**
 * Helper functions to create common schemas for easier migration
 */
export declare class SchemaHelpers {
    /**
     * Create a schema that accepts any parameters and returns raw Value objects
     */
    static anySchema(): BidirectionalSchema;
    /**
     * Create a schema for named parameters with auto-detection
     */
    static namedParams(): BidirectionalSchema;
    /**
     * Create a schema for array parameters with auto-detection
     */
    static arrayParams(): BidirectionalSchema;
    /**
     * Create a schema that returns primitive values for common types
     */
    static primitiveResults(): BidirectionalSchema;
    /**
     * Create a flexible schema for testing that accepts anything
     */
    static flexibleSchema(): BidirectionalSchema;
}
export declare const DEFAULT_SCHEMA: BidirectionalSchema;
