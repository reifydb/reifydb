// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import type {
    LoginResult,
    ResponseMeta,
} from "./types";
import {
    ReifyError
} from "./types";
import {encode_params} from "./encoder";
import {CONTENT_TYPE_JSON} from "./content-types";

export interface JsonHttpClientOptions {
    url: string;
    timeout_ms?: number;
    token?: string;
    unwrap?: boolean;
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
        return this.login("password", {identifier: identity, password}, req_opts);
    }

    async login_with_token(token: string, req_opts?: RequestOptions): Promise<LoginResult> {
        return this.login("token", {token}, req_opts);
    }

    async login(method: string, credentials: Record<string, string>, req_opts?: RequestOptions): Promise<LoginResult> {
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
                body: JSON.stringify({method, credentials}),
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
        const { data } = await this.admin_with_meta(statements, params, req_opts);
        return data;
    }

    async admin_with_meta(
        statements: string | string[],
        params?: any,
        req_opts?: RequestOptions
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute('admin', statements, params, req_opts);
    }

    async command(
        statements: string | string[],
        params?: any,
        req_opts?: RequestOptions
    ): Promise<any> {
        const { data } = await this.command_with_meta(statements, params, req_opts);
        return data;
    }

    async command_with_meta(
        statements: string | string[],
        params?: any,
        req_opts?: RequestOptions
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute('command', statements, params, req_opts);
    }

    async query(
        statements: string | string[],
        params?: any,
        req_opts?: RequestOptions
    ): Promise<any> {
        const { data } = await this.query_with_meta(statements, params, req_opts);
        return data;
    }

    async query_with_meta(
        statements: string | string[],
        params?: any,
        req_opts?: RequestOptions
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute('query', statements, params, req_opts);
    }

    private async execute(
        endpoint: 'admin' | 'command' | 'query',
        statements: string | string[],
        params: any,
        req_opts?: RequestOptions
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        return this.send(endpoint, output_statements, encoded_params, req_opts);
    }

    private async send(
        endpoint: string,
        statements: string[],
        params: any,
        req_opts?: RequestOptions,
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        const timeout_ms = this.options.timeout_ms ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeout_ms);

        let signal = controller.signal;
        if (req_opts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, req_opts.signal]);
        } else if (req_opts?.signal) {
            req_opts.signal.addEventListener('abort', () => controller.abort());
        }

        // JsonHttpClient with unwrap mode also accepts raw `application/json` passthrough.
        const headers: Record<string, string> = {
            'Content-Type': 'application/json',
            'Accept': `${CONTENT_TYPE_JSON}, application/json`,
        };

        if (this.options.token) {
            headers['Authorization'] = `Bearer ${this.options.token}`;
        }

        const body: any = {statements};
        if (params !== undefined) {
            body.params = params;
        }

        const query_params = new URLSearchParams({format: 'json'});
        if (this.options.unwrap) {
            query_params.set('unwrap', 'true');
        }
        const url = `${this.options.url}/v1/${endpoint}?${query_params}`;

        try {
            const response = await fetch(url, {
                method: 'POST',
                headers,
                body: JSON.stringify(body),
                signal,
                credentials: 'include',
            });

            clearTimeout(timeout);

            const meta = extract_meta(response.headers);

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

            return { data: parsed, meta };
        } catch (err: any) {
            clearTimeout(timeout);
            if (err.name === 'AbortError') {
                throw new Error("ReifyDB query timeout");
            }
            throw err;
        }
    }
}

function extract_meta(headers: Headers | undefined): ResponseMeta | undefined {
    const fingerprint = headers?.get?.('x-fingerprint');
    const duration = headers?.get?.('x-duration');
    if (!fingerprint || !duration) return undefined;
    return { fingerprint, duration };
}
