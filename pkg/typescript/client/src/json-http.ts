// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {decode} from "@reifydb/core";
import type {
    Column,
    LoginResult,
} from "./types";
import {
    ReifyError
} from "./types";
import {encode_params} from "./encoder";
import {rbcf} from "./rbcf";

export interface JsonHttpClientOptions {
    url: string;
    timeout_ms?: number;
    token?: string;
    unwrap?: boolean;
    /** Wire-format encoding for data frames. Defaults to "json". */
    encoding?: "json" | "rbcf";
}

export interface RequestOptions {
    signal?: AbortSignal;
}

export class JsonHttpClient {
    private options: JsonHttpClientOptions;

    private constructor(options: JsonHttpClientOptions) {
        this.options = options;
    }

    static connect(options: JsonHttpClientOptions): JsonHttpClient {
        return new JsonHttpClient(options);
    }

    async login_with_password(identity: string, password: string, req_opts?: RequestOptions): Promise<LoginResult> {
        return this.login("password", identity, {password}, req_opts);
    }

    async login_with_token(identity: string, token: string, req_opts?: RequestOptions): Promise<LoginResult> {
        return this.login("token", identity, {token}, req_opts);
    }

    async login(method: string, identity: string, credentials: Record<string, string>, req_opts?: RequestOptions): Promise<LoginResult> {
        const timeout_ms = this.options.timeout_ms ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeout_ms);

        let signal = controller.signal;
        if (req_opts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, req_opts.signal]);
        } else if (req_opts?.signal) {
            req_opts.signal.addEventListener('abort', () => controller.abort());
        }

        try {
            const response = await fetch(`${this.options.url}/v1/authenticate`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({method, credentials: {identifier: identity, ...credentials}}),
                signal,
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
            if (err.name === 'AbortError') throw new Error("Login timeout or aborted");
            throw err;
        }
    }

    async logout(req_opts?: RequestOptions): Promise<void> {
        if (!this.options.token) {
            return;
        }

        const timeout_ms = this.options.timeout_ms ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeout_ms);

        let signal = controller.signal;
        if (req_opts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, req_opts.signal]);
        } else if (req_opts?.signal) {
            req_opts.signal.addEventListener('abort', () => controller.abort());
        }

        try {
            const response = await fetch(`${this.options.url}/v1/logout`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.options.token}`,
                },
                signal,
            });

            clearTimeout(timeout);

            if (!response.ok) {
                const body = await response.text();
                throw new Error(`Logout failed: HTTP ${response.status}: ${body}`);
            }

            this.options = {...this.options, token: undefined};
        } catch (err: any) {
            clearTimeout(timeout);
            if (err.name === 'AbortError') throw new Error("Logout timeout or aborted");
            throw err;
        }
    }

    async admin(
        statements: string | string[],
        params?: any,
        req_opts?: RequestOptions
    ): Promise<any> {
        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        return this.send('admin', output_statements, encoded_params, req_opts);
    }

    async command(
        statements: string | string[],
        params?: any,
        req_opts?: RequestOptions
    ): Promise<any> {
        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        return this.send('command', output_statements, encoded_params, req_opts);
    }

    async query(
        statements: string | string[],
        params?: any,
        req_opts?: RequestOptions
    ): Promise<any> {
        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        return this.send('query', output_statements, encoded_params, req_opts);
    }

    private async send(endpoint: string, statements: string[], params: any, req_opts?: RequestOptions): Promise<any> {
        const timeout_ms = this.options.timeout_ms ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeout_ms);

        let signal = controller.signal;
        if (req_opts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, req_opts.signal]);
        } else if (req_opts?.signal) {
            req_opts.signal.addEventListener('abort', () => controller.abort());
        }

        const use_rbcf = this.options.encoding === "rbcf";
        const headers: Record<string, string> = {
            'Content-Type': 'application/json',
        };
        if (use_rbcf) {
            headers['Accept'] = 'application/rbcf, application/json';
        }

        if (this.options.token) {
            headers['Authorization'] = `Bearer ${this.options.token}`;
        }

        const body: any = {statements};
        if (params !== undefined) {
            body.params = params;
        }

        const query_params = new URLSearchParams({format: use_rbcf ? 'rbcf' : 'json'});
        if (this.options.unwrap) {
            query_params.set('unwrap', 'true');
        }

        try {
            const response = await fetch(`${this.options.url}/v1/${endpoint}?${query_params}`, {
                method: 'POST',
                headers,
                body: JSON.stringify(body),
                signal,
                credentials: 'include',
            });

            clearTimeout(timeout);

            const content_type = response.headers?.get?.('content-type') ?? '';
            const is_binary = use_rbcf && response.ok &&
                (content_type.includes('application/rbcf') || content_type.includes('application/octet-stream'));

            if (is_binary) {
                const buf = await response.arrayBuffer();
                const frames = rbcf.decode(new Uint8Array(buf));
                return frames.map((frame: any) => columns_to_plain_rows(frame.columns));
            }

            const response_body = await response.text();
            let parsed: any;
            try {
                parsed = JSON.parse(response_body);
            } catch {
                throw new Error(`Invalid JSON response: ${response_body}`);
            }

            if (!response.ok) {
                if (parsed.diagnostic) {
                    throw new ReifyError({
                        id: '',
                        type: 'Err',
                        payload: {diagnostic: parsed.diagnostic}
                    });
                }
                throw new Error(parsed.error || `HTTP ${response.status}: ${response_body}`);
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

function columns_to_plain_rows(columns: Column[]): Record<string, any>[] {
    const row_count = columns[0]?.payload.length ?? 0;
    return Array.from({length: row_count}, (_, i) => {
        const row: Record<string, any> = {};
        for (const col of columns) {
            const value = decode({type: col.type, value: col.payload[i]});
            row[col.name] = value?.valueOf();
        }
        return row;
    });
}
