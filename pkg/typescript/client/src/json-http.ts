// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import type {
    LoginResult,
} from "./types";
import {
    ReifyError
} from "./types";
import {encodeParams} from "./encoder";

export interface JsonHttpClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
    unwrap?: boolean;
}

export class JsonHttpClient {
    private options: JsonHttpClientOptions;

    private constructor(options: JsonHttpClientOptions) {
        this.options = options;
    }

    static connect(options: JsonHttpClientOptions): JsonHttpClient {
        return new JsonHttpClient(options);
    }

    async loginWithPassword(principal: string, password: string): Promise<LoginResult> {
        return this.login("password", principal, {password});
    }

    async loginWithToken(principal: string, token: string): Promise<LoginResult> {
        return this.login("token", principal, {token});
    }

    async login(method: string, principal: string, credentials: Record<string, string>): Promise<LoginResult> {
        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(`${this.options.url}/v1/authenticate`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({method, principal, credentials}),
                signal: controller.signal,
            });

            clearTimeout(timeout);
            const body = await response.json();

            if (body.status !== "authenticated" || !body.token || !body.identity) {
                throw new Error(body.reason || "Authentication failed");
            }

            this.options = {...this.options, token: body.token};

            return {token: body.token, identity: body.identity};
        } catch (err: any) {
            clearTimeout(timeout);
            if (err.name === 'AbortError') throw new Error("Login timeout");
            throw err;
        }
    }

    async logout(): Promise<void> {
        if (!this.options.token) {
            return;
        }

        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(`${this.options.url}/v1/logout`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.options.token}`,
                },
                signal: controller.signal,
            });

            clearTimeout(timeout);

            if (!response.ok) {
                const body = await response.text();
                throw new Error(`Logout failed: HTTP ${response.status}: ${body}`);
            }

            this.options = {...this.options, token: undefined};
        } catch (err: any) {
            clearTimeout(timeout);
            if (err.name === 'AbortError') throw new Error("Logout timeout");
            throw err;
        }
    }

    async admin(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        return this.send('admin', outputStatements, encodedParams);
    }

    async command(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        return this.send('command', outputStatements, encodedParams);
    }

    async query(
        statements: string | string[],
        params?: any,
    ): Promise<any> {
        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        return this.send('query', outputStatements, encodedParams);
    }

    private async send(endpoint: string, statements: string[], params: any): Promise<any> {
        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        const headers: Record<string, string> = {
            'Content-Type': 'application/json',
        };

        if (this.options.token) {
            headers['Authorization'] = `Bearer ${this.options.token}`;
        }

        const body: any = {statements};
        if (params !== undefined) {
            body.params = params;
        }

        const queryParams = new URLSearchParams({format: 'json'});
        if (this.options.unwrap) {
            queryParams.set('unwrap', 'true');
        }

        try {
            const response = await fetch(`${this.options.url}/v1/${endpoint}?${queryParams}`, {
                method: 'POST',
                headers,
                body: JSON.stringify(body),
                signal: controller.signal,
                credentials: 'include',
            });

            clearTimeout(timeout);

            const responseBody = await response.text();
            let parsed: any;
            try {
                parsed = JSON.parse(responseBody);
            } catch {
                throw new Error(`Invalid JSON response: ${responseBody}`);
            }

            if (!response.ok) {
                if (parsed.diagnostic) {
                    throw new ReifyError({
                        id: '',
                        type: 'Err',
                        payload: {diagnostic: parsed.diagnostic}
                    });
                }
                throw new Error(parsed.error || `HTTP ${response.status}: ${responseBody}`);
            }

            return parsed;
        } catch (err: any) {
            clearTimeout(timeout);
            if (err.name === 'AbortError') {
                throw new Error("ReifyDB query timeout");
            }
            throw err;
        }
    }
}
