import {useEffect} from 'react';
import {SchemaNode} from '@reifydb/core';
import {useQueryExecutor} from "@/hooks/use-query-executor.ts";

export function useQuery<T = any>(rql: string, schema?: SchemaNode) {
    const {
        isExecuting,
        result,
        error,
        query
    } = useQueryExecutor<T>();

    useEffect(() => {
        query(rql, schema);
    }, [rql, schema, query]);

    return {isExecuting, result, error};
}
