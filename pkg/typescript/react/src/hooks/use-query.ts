import {useEffect, useMemo} from 'react';
import {SchemaNode, InferSchema} from '@reifydb/core';
import {useQueryExecutor, type QueryResult} from './use-query-executor';

// Single query hook - returns a single result
export function useQueryOne<S extends SchemaNode = any>(
    rql: string,
    params?: any,
    schema?: S
): {
    isExecuting: boolean;
    result: QueryResult<S extends SchemaNode ? InferSchema<S> : any> | undefined;
    error: string | undefined;
} {
    const {
        isExecuting,
        results,
        error,
        query
    } = useQueryExecutor<S extends SchemaNode ? InferSchema<S> : any>();

    useEffect(() => {
        // Pass schema as array for the executor
        const schemas = schema ? [schema] : undefined;
        query(rql, params, schemas);
    }, [rql, params, query]);

    // Extract first result for single query convenience
    const result = useMemo(() => {
        return results && results.length > 0 ? results[0] : undefined;
    }, [results]);

    return {isExecuting, result, error};
}

// Multiple query hook - returns multiple results
export function useQueryMany<S extends readonly SchemaNode[] = readonly SchemaNode[]>(
    statements: string | string[],
    params?: any,
    schemas?: S
): {
    isExecuting: boolean;
    results: QueryResult<S extends readonly SchemaNode[] ? InferSchema<S[number]> : any>[] | undefined;
    error: string | undefined;
} {
    const {
        isExecuting,
        results,
        error,
        query
    } = useQueryExecutor<S extends readonly SchemaNode[] ? InferSchema<S[number]> : any>();

    useEffect(() => {
        query(statements, params, schemas);
    }, [statements, params, query]);

    return {isExecuting, results, error};
}