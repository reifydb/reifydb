// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { describe, it, expect } from 'vitest';
import { createWasmDB } from '../src/index';

describe('auth', () => {
  it('login with password', async () => {
    const db = await createWasmDB();

    db.admin("CREATE USER alice");
    db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }");

    const result = db.loginWithPassword('alice', 'alice-pass');
    expect(result.token).toBeTruthy();
    expect(result.identity).toBeTruthy();

    db.free();
  });

  it('login with wrong password fails', async () => {
    const db = await createWasmDB();

    db.admin("CREATE USER alice");
    db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }");

    expect(() => db.loginWithPassword('alice', 'wrong-password')).toThrow();

    db.free();
  });

  it('login with token', async () => {
    const db = await createWasmDB();

    db.admin("CREATE USER bob");
    db.admin("CREATE AUTHENTICATION FOR bob { method: token; token: 'bob-secret-token' }");

    const result = db.loginWithToken('bob-secret-token');
    expect(result.token).toBeTruthy();
    expect(result.identity).toBeTruthy();

    db.free();
  });

  it('login with invalid token fails', async () => {
    const db = await createWasmDB();

    db.admin("CREATE USER bob");
    db.admin("CREATE AUTHENTICATION FOR bob { method: token; token: 'bob-secret-token' }");

    expect(() => db.loginWithToken('wrong-token')).toThrow();

    db.free();
  });

  it('logout after login', async () => {
    const db = await createWasmDB();

    db.admin("CREATE USER alice");
    db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }");

    db.loginWithPassword('alice', 'alice-pass');
    db.logout();

    db.free();
  });

  it('logout without login is a no-op', async () => {
    const db = await createWasmDB();
    db.logout();
    db.free();
  });
});
