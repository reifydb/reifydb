// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

export * from '@reifydb/core';
export * from '@reifydb/client';

// Export connection utilities
export {Connection, type ConnectionConfig, DEFAULT_CONFIG} from './connection/connection';
export {getConnection, clearConnection} from './connection/connection-pool';
export {ConnectionProvider, ConnectionContext, type ConnectionProviderProps} from './connection/connection-context';

// Export React hooks
export {useConnection} from './hooks/use-connection';
export {useQueryExecutor, type QueryResult, type QueryState, type QueryExecutorOptions} from './hooks/use-query-executor';
export {useQueryOne, useQueryMany, type QueryOptions} from './hooks/use-query';
export {useCommandExecutor, type CommandResult, type CommandState, type CommandExecutorOptions} from './hooks/use-command-executor';
export {useCommandOne, useCommandMany, type CommandOptions} from './hooks/use-command';
export {
    useSubscriptionExecutor,
    type SubscriptionExecutorOptions,
    type SubscriptionState,
    type ChangeEvent
} from './hooks/use-subscription-executor';
export {
    useSubscription,
    type SubscriptionOptions
} from './hooks/use-subscription';
export {useSchema, type TableInfo, type ColumnInfo} from './hooks/use-schema';