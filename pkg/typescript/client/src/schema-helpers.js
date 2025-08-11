"use strict";
/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_SCHEMA = exports.SchemaHelpers = void 0;
var core_1 = require("@reifydb/core");
/**
 * Helper functions to create common schemas for easier migration
 */
var SchemaHelpers = /** @class */ (function () {
    function SchemaHelpers() {
    }
    /**
     * Create a schema that accepts any parameters and returns raw Value objects
     */
    SchemaHelpers.anySchema = function () {
        return {
            params: core_1.Schema.optional(core_1.Schema.auto()),
            // No result schema - returns Value objects
        };
    };
    /**
     * Create a schema for named parameters with auto-detection
     */
    SchemaHelpers.namedParams = function () {
        return {
            params: core_1.Schema.object({}), // Empty object schema allows any properties
        };
    };
    /**
     * Create a schema for array parameters with auto-detection
     */
    SchemaHelpers.arrayParams = function () {
        return {
            params: core_1.Schema.array(core_1.Schema.auto()),
        };
    };
    /**
     * Create a schema that returns primitive values for common types
     */
    SchemaHelpers.primitiveResults = function () {
        return {
            result: core_1.Schema.auto() // Auto-detect and return primitives
        };
    };
    /**
     * Create a flexible schema for testing that accepts anything
     */
    SchemaHelpers.flexibleSchema = function () {
        return {
            params: core_1.Schema.auto(),
            result: core_1.Schema.auto()
        };
    };
    return SchemaHelpers;
}());
exports.SchemaHelpers = SchemaHelpers;
// Legacy compatibility - create a default flexible schema 
exports.DEFAULT_SCHEMA = SchemaHelpers.flexibleSchema();
