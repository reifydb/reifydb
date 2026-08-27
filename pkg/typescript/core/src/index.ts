// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
export {NONE_VALUE, ROW_NUMBER_KEY} from './constant';
export {decode, columnsToRows} from './decoder';
export {encodeValue, encodeParams} from './encoder';
export {TYPE_CODE, typeNameFromCode} from './type-code';
export type {TypeName} from './type-code';
export * from './shape'
export * from './value';
export type {
    Params,
    Frame,
    DiagnosticColumn,
    Fragment,
    Diagnostic,
    Column,
    ErrorResponse,
    FrameResults,
    SingleFrameResult,
} from './types';
export {
    ReifyError,
    asFrameResults
} from './types';

