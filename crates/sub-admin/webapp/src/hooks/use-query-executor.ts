import {useState, useCallback, useRef} from 'react';
import {Column, FrameResults, Value, SchemaNode, InferSchema} from '@reifydb/core';
import {useConnection} from "@/hooks/use-connection.ts";

export interface QueryResult<T = any> {
    columns: Column[];
    rows: T[];
    executionTimeMs: number;
    rowsAffected?: number;
}

export interface QueryState<T = any> {
    isExecuting: boolean;
    result: QueryResult<T> | undefined;
    error: string | undefined;
    executionTime: number | undefined;
}

export function useQueryExecutor<T = any>() {
    const {client} = useConnection();

    const [state, setState] = useState<QueryState<T>>({
        isExecuting: false,
        result: undefined,
        error: undefined,
        executionTime: undefined,
    });

    const abortControllerRef = useRef<AbortController | null>(null);

    const query = useCallback(
        <S extends SchemaNode>(rql: string, schema?: S): void => {
            console.log('[useQuery] Executing query:', rql);
            console.log('[useQuery] Client available:', !!client);

            // Cancel any ongoing query for THIS instance only
            if (abortControllerRef.current) {
                abortControllerRef.current.abort();
            }
            abortControllerRef.current = new AbortController();

            setState({
                isExecuting: true,
                result: undefined,
                error: undefined,
                executionTime: undefined,
            });

            const startTime = Date.now();

            (async () => {
                try {
                    const schemas = schema ? [schema] : [];
                    const frames = await client?.query(rql, null, schemas) || [];

                    console.debug("frames", frames);

                    const executionTime = Date.now() - startTime;

                    let queryResult: QueryResult<T>;
                    const firstFrame = frames.length > 0 ? frames[0] : undefined;

                    if (Array.isArray(firstFrame) && firstFrame.length > 0) {
                        const firstRow = firstFrame[0];
                        let columns: Column[] = [];
                        let rows: T[];

                        if (schema) {
                            // With schema, results are properly typed
                            columns = Object.keys(firstRow).map((key) => ({
                                name: key,
                                type: 'Utf8', // Type info would come from schema
                                data: [],
                            }));
                            rows = firstFrame as T[];
                        } else {
                            // Without schema, we have Value objects
                            columns = Object.keys(firstRow).map((key) => {
                                const value = firstRow[key];
                                const dataType = value?.type || 'Utf8';
                                return {
                                    name: key,
                                    type: dataType,
                                    data: [],
                                };
                            });
                            rows = firstFrame as T[];
                        }

                        queryResult = {
                            columns,
                            rows,
                            executionTimeMs: executionTime,
                        };
                    } else {
                        // Empty result set or no frames returned
                        queryResult = {
                            columns: [],
                            rows: [],
                            executionTimeMs: executionTime,
                        };
                    }

                    setState({
                        isExecuting: false,
                        result: queryResult,
                        error: undefined,
                        executionTime,
                    });
                } catch (err) {
                    const executionTime = Date.now() - startTime;
                    let errorMessage = 'Query execution failed';

                    if (err instanceof Error) {
                        errorMessage = err.message;
                    } else if (typeof err === 'string') {
                        errorMessage = err;
                    } else if (err && typeof err === 'object' && 'message' in err) {
                        errorMessage = (err as { message: string }).message;
                    }

                    setState({
                        isExecuting: false,
                        result: undefined,
                        error: errorMessage,
                        executionTime,
                    });
                } finally {
                    abortControllerRef.current = null;
                }
            })();
        },
        [client]
    );

    const cancelQuery = useCallback(() => {
        if (abortControllerRef.current) {
            abortControllerRef.current.abort();
            setState((prev) => ({
                ...prev,
                isExecuting: false,
                error: 'Query cancelled',
            }));
        }
    }, []);

    return {
        // State
        isExecuting: state.isExecuting,
        result: state.result,
        error: state.error,
        executionTime: state.executionTime,

        // Actions
        query,
        cancelQuery,
    };
}
