// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

const QUERY_TRANSFORMS = [
    'map', 'extend', 'by', 'from', 'where', 'aggregate', 'having',
    'sort', 'distinct', 'take', 'offset',
];

const JOINS_AND_SET_OPERATIONS = [
    'left', 'inner', 'natural', 'join', 'on', 'using', 'intersect', 'except',
];

const DML = ['insert', 'into', 'update', 'set', 'delete'];

const CONTROL_FLOW = [
    'let', 'if', 'else', 'end', 'loop', 'while', 'break', 'continue', 'return',
];

const FUNCTIONS_AND_CASTING = ['fun', 'call', 'apply', 'cast'];

const DDL_AND_SHAPE = [
    'describe', 'show', 'create', 'alter', 'drop', 'filter', 'gate', 'flow', 'window',
    'returning',
];

const OPERATOR_KEYWORDS = ['in', 'between', 'like', 'is', 'with'];

const OBJECT_TYPES = [
    'namespace', 'sequence', 'series', 'subscription', 'table', 'ringbuffer',
    'column', 'policy', 'property', 'view', 'deferred', 'transactional',
];

const INDEX_AND_CONSTRAINTS = [
    'index', 'unique', 'primary', 'key', 'asc', 'desc', 'auto', 'increment', 'value',
];

const MISC_OPERATIONS = [
    'exists', 'replace', 'cascade', 'restrict', 'to', 'pause', 'resume',
    'query', 'rename', 'rownum', 'dictionary', 'for', 'output', 'append',
    'assert', 'patch',
];

const ENUMS_AND_PATTERN_MATCHING = ['enum', 'match'];

const PROCEDURES_AND_EVENTS = ['procedure', 'event', 'handler', 'dispatch', 'tag'];

const TESTING = ['test', 'tests', 'run'];

const ACCESS_CONTROL = [
    'user', 'role', 'grant', 'revoke', 'password', 'require', 'execute',
    'access', 'subscribe', 'enable', 'disable',
];

const SYSTEM_OBJECTS = ['function', 'session', 'feature'];

const MIGRATIONS = [
    'add', 'migration', 'migrate', 'rollback', 'diff', 'version', 'current', 'pending',
];

const MISC = ['authentication', 'contains', 'remote', 'error'];

const ADDITIONAL_CONSTRUCTS = ['derive', 'group', 'union', 'as'];

const WORD_OPERATORS = ['and', 'or', 'not', 'xor'];

export const RQL_KEYWORDS: readonly string[] = [
    ...QUERY_TRANSFORMS,
    ...JOINS_AND_SET_OPERATIONS,
    ...DML,
    ...CONTROL_FLOW,
    ...FUNCTIONS_AND_CASTING,
    ...DDL_AND_SHAPE,
    ...OPERATOR_KEYWORDS,
    ...OBJECT_TYPES,
    ...INDEX_AND_CONSTRAINTS,
    ...MISC_OPERATIONS,
    ...ENUMS_AND_PATTERN_MATCHING,
    ...PROCEDURES_AND_EVENTS,
    ...TESTING,
    ...ACCESS_CONTROL,
    ...SYSTEM_OBJECTS,
    ...MIGRATIONS,
    ...MISC,
    ...ADDITIONAL_CONSTRUCTS,
    ...WORD_OPERATORS,
];

export const RQL_LITERALS: readonly string[] = ['none', 'true', 'false'];

const SIGNED_INTEGERS = ['int1', 'int2', 'int4', 'int8', 'int16', 'int'];

const UNSIGNED_INTEGERS = ['uint1', 'uint2', 'uint4', 'uint8', 'uint16', 'uint'];

const FLOATING_POINT = ['float4', 'float8'];

const TEXT_AND_BINARY = ['utf8', 'blob', 'text'];

const BOOLEAN = ['bool', 'boolean'];

const NUMERIC = ['decimal'];

const TEMPORAL = ['date', 'datetime', 'time', 'duration', 'interval'];

const IDENTIFIERS_AND_UUIDS = [
    'uuid4', 'uuid7', 'identityid', 'identity_id', 'dictionaryid', 'dictionary_id',
];

const CONTAINERS = ['Option', 'List', 'Record', 'Tuple'];

export const RQL_TYPES: readonly string[] = [
    ...SIGNED_INTEGERS,
    ...UNSIGNED_INTEGERS,
    ...FLOATING_POINT,
    ...TEXT_AND_BINARY,
    ...BOOLEAN,
    ...NUMERIC,
    ...TEMPORAL,
    ...IDENTIFIERS_AND_UUIDS,
    ...CONTAINERS,
    'any',
];

export const RQL_OPERATORS: readonly string[] = [
    '+', '-', '*', '/', '//', '%', '=', '==', '!=', '->', '=>', '>', '<', '>=', '<=',
    '~=', '&&', '||', '??',
];

export const RQL_SYSTEM_COLUMNS: readonly string[] = ['rownum', 'created_at', 'updated_at'];
