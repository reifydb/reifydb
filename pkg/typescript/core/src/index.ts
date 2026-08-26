// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
export {NONE_VALUE} from './constant';
export {decode, columns_to_rows} from './decoder';
export {encode_value, encode_params} from './encoder';
export {TYPE_CODE, type_name_from_code} from './type-code';
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

