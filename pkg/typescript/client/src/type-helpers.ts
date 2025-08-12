/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { SchemaNode, InferSchema } from "@reifydb/core";

/**
 * Simplified type for inferring frame results to avoid deep instantiation
 * This provides a workaround for TypeScript's limitation with deeply nested types
 */
export type FrameResults<S extends readonly SchemaNode[]> = 
    S extends readonly [infer First, ...infer Rest]
        ? First extends SchemaNode
            ? Rest extends readonly SchemaNode[]
                ? [InferSchema<First>[], ...FrameResults<Rest>]
                : [InferSchema<First>[]]
            : never
        : [];

/**
 * Helper type to extract a single frame result
 */
export type SingleFrameResult<S extends SchemaNode> = InferSchema<S>[];

/**
 * Type-safe cast helper for frame results
 */
export function asFrameResults<S extends readonly SchemaNode[]>(
    results: any
): FrameResults<S> {
    return results as FrameResults<S>;
}