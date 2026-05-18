// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, it, expect } from 'vitest';
import { transform_to_value_instances, WasmDB } from '../src/index';

describe('transform_to_value_instances', () => {
  it('passes through null', () => {
    expect(transform_to_value_instances(null)).toBeNull();
  });

  it('passes through undefined', () => {
    expect(transform_to_value_instances(undefined)).toBeUndefined();
  });

  it('passes through primitives', () => {
    expect(transform_to_value_instances(42)).toBe(42);
    expect(transform_to_value_instances('hello')).toBe('hello');
    expect(transform_to_value_instances(true)).toBe(true);
  });

  it('decodes a {type, value} pair', () => {
    const pair = { type: 'Utf8', value: 'hello' };
    const result = transform_to_value_instances(pair);
    expect(result).toBeDefined();
    expect(result).toBeInstanceOf(Object);
    expect((result as any).value).toBe('hello');
  });

  it('decodes nested arrays of {type, value} pairs', () => {
    const input = [
      { type: 'Int4', value: '42' },
      { type: 'Utf8', value: 'test' },
    ];
    const result = transform_to_value_instances(input) as unknown[];
    expect(result).toHaveLength(2);
    expect(result[0]).toBeDefined();
    expect(result[1]).toBeDefined();
  });

  it('transforms object properties recursively', () => {
    const input = {
      name: { type: 'Utf8', value: 'Alice' },
      age: { type: 'Int4', value: '30' },
    };
    const result = transform_to_value_instances(input) as Record<string, unknown>;
    expect(result.name).toBeDefined();
    expect(result.age).toBeDefined();
  });

  it('handles mixed nested structures', () => {
    const input = [
      {
        name: { type: 'Utf8', value: 'Alice' },
        scores: [
          { type: 'Int4', value: '100' },
          { type: 'Int4', value: '95' },
        ],
      },
    ];
    const result = transform_to_value_instances(input) as Record<string, unknown>[];
    expect(result).toHaveLength(1);
    expect(result[0].name).toBeDefined();
    expect(result[0].scores).toHaveLength(2);
  });

  it('preserves objects that are not {type, value} pairs', () => {
    const input = { type: 'Utf8', value: 'hello', extra: true };
    const result = transform_to_value_instances(input) as Record<string, unknown>;
    // Has 3 keys, so not treated as a TypeValuePair — properties transformed individually
    expect(result.extra).toBe(true);
  });
});

describe('WasmDB', () => {
  function create_mock_login_result(token = 'mock-token', identity = 'mock-identity') {
    return { token, identity, free: () => {} };
  }

  function create_mock_raw_db() {
    return {
      admin: (rql: string) => [{ type: 'Utf8', value: rql }],
      adminWithParams: (rql: string, _params: unknown) => [{ type: 'Utf8', value: rql }],
      command: (rql: string) => [{ type: 'Utf8', value: rql }],
      commandWithParams: (rql: string, _params: unknown) => [{ type: 'Utf8', value: rql }],
      query: (rql: string) => [{ type: 'Utf8', value: rql }],
      queryWithParams: (rql: string, _params: unknown) => [{ type: 'Utf8', value: rql }],
      loginWithPassword: (_identifier: string, _password: string) => create_mock_login_result('pw-token', 'alice-id'),
      loginWithToken: (_token: string) => create_mock_login_result('tk-token', 'bob-id'),
      logout: () => {},
      free: () => {},
    };
  }

  it('wraps admin calls and transforms results', () => {
    const db = new WasmDB(create_mock_raw_db() as any);
    const result = db.admin('CREATE NAMESPACE demo') as unknown[];
    expect(result).toHaveLength(1);
    expect(result[0]).toBeDefined();
  });

  it('wraps command calls and transforms results', () => {
    const db = new WasmDB(create_mock_raw_db() as any);
    const result = db.command('INSERT demo.users [{}]') as unknown[];
    expect(result).toHaveLength(1);
  });

  it('wraps query calls and transforms results', () => {
    const db = new WasmDB(create_mock_raw_db() as any);
    const result = db.query('FROM demo.users') as unknown[];
    expect(result).toHaveLength(1);
  });

  it('wraps WithParams variants', () => {
    const db = new WasmDB(create_mock_raw_db() as any);
    const result = db.query_with_params('FROM users FILTER id = $id', { id: 1 }) as unknown[];
    expect(result).toHaveLength(1);
  });

  it('loginWithPassword delegates to raw and returns plain object', () => {
    const db = new WasmDB(create_mock_raw_db() as any);
    const result = db.login_with_password('alice', 'alice-pass');
    expect(result.token).toBe('pw-token');
    expect(result.identity).toBe('alice-id');
  });

  it('loginWithToken delegates to raw and returns plain object', () => {
    const db = new WasmDB(create_mock_raw_db() as any);
    const result = db.login_with_token('bob-secret-token');
    expect(result.token).toBe('tk-token');
    expect(result.identity).toBe('bob-id');
  });

  it('loginWithPassword frees the raw LoginResult', () => {
    let freed = false;
    const mock = {
      ...create_mock_raw_db(),
      loginWithPassword: () => ({ token: 't', identity: 'i', free: () => { freed = true; } }),
    };
    const db = new WasmDB(mock as any);
    db.login_with_password('alice', 'pass');
    expect(freed).toBe(true);
  });

  it('logout delegates to raw', () => {
    let loggedOut = false;
    const mock = { ...create_mock_raw_db(), logout: () => { loggedOut = true; } };
    const db = new WasmDB(mock as any);
    db.logout();
    expect(loggedOut).toBe(true);
  });

  it('delegates free to raw db', () => {
    let freed = false;
    const mock = { ...create_mock_raw_db(), free: () => { freed = true; } };
    const db = new WasmDB(mock as any);
    db.free();
    expect(freed).toBe(true);
  });
});
