// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {
    decode,
    Value
} from "@reifydb/core";
import type {
    ShapeNode,
    FrameResults,
} from "@reifydb/core";

import type {
    Column,
    LoginResult,
} from "./types";
import {
    ReifyError
} from "./types";
import {encode_params} from "./encoder";
import {rbcf} from "./rbcf";

export interface HttpClientOptions {
    url: string;
    timeout_ms?: number;
    token?: string;
    /** Wire-format encoding for data frames. Defaults to "json". */
    encoding?: "json" | "rbcf";
}

export interface RequestOptions {
    signal?: AbortSignal;
}

export class HttpClient {
    private options: HttpClientOptions;

    private constructor(options: HttpClientOptions) {
        this.options = options;
    }

    static connect(options: HttpClientOptions): HttpClient {
        return new HttpClient(options);
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
            // Polyfill or fallback if AbortSignal.any is missing
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

    async admin<const S extends readonly ShapeNode[]>(
        statements: string | string[],
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        const result = await this.send('admin', output_statements, encoded_params, req_opts);

        const transformed_frames = result.map((frame: any, frame_index: number) => {
            const frame_shape = shapes[frame_index];
            if (!frame_shape) {
                return frame;
            }
            return frame.map((row: any) => this.transform_result(row, frame_shape));
        });

        return transformed_frames as FrameResults<S>;
    }

    async command<const S extends readonly ShapeNode[]>(
        statements: string | string[],
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        const result = await this.send('command', output_statements, encoded_params, req_opts);

        const transformed_frames = result.map((frame: any, frame_index: number) => {
            const frame_shape = shapes[frame_index];
            if (!frame_shape) {
                return frame;
            }
            return frame.map((row: any) => this.transform_result(row, frame_shape));
        });

        return transformed_frames as FrameResults<S>;
    }

    async query<const S extends readonly ShapeNode[]>(
        statements: string | string[],
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const statement_array = Array.isArray(statements) ? statements : [statements];
        const output_statements = statement_array.length > 1
            ? statement_array.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statement_array;

        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        const result = await this.send('query', output_statements, encoded_params, req_opts);

        const transformed_frames = result.map((frame: any, frame_index: number) => {
            const frame_shape = shapes[frame_index];
            if (!frame_shape) {
                return frame;
            }
            return frame.map((row: any) => this.transform_result(row, frame_shape));
        });

        return transformed_frames as FrameResults<S>;
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

        const url = use_rbcf
            ? `${this.options.url}/v1/${endpoint}?format=rbcf`
            : `${this.options.url}/v1/${endpoint}`;

        try {
            const response = await fetch(url, {
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
                return frames.map((frame: any) => columns_to_rows(frame.columns));
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

            const frames = parsed.frames || [];
            return frames.map((frame: any) =>
                columns_to_rows(frame.columns)
            );
        } catch (err: any) {
            clearTimeout(timeout);
            if (err.name === 'AbortError') {
                throw new Error("ReifyDB query timeout");
            }
            throw err;
        }
    }

    private transform_result(row: any, result_shape: any): any {
        if (result_shape && result_shape.kind === 'object' && result_shape.properties) {
            const transformed_row: any = {};
            for (const [key, value] of Object.entries(row)) {
                const property_shape = result_shape.properties[key];
                if (property_shape && property_shape.kind === 'primitive') {
                    if (value && typeof value === 'object' && typeof (value as any).valueOf === 'function') {
                        const raw_value = (value as any).valueOf();
                        transformed_row[key] = this.coerce_to_primitive_type(raw_value, property_shape.type);
                    } else {
                        transformed_row[key] = this.coerce_to_primitive_type(value, property_shape.type);
                    }
                } else if (property_shape && property_shape.kind === 'value') {
                    transformed_row[key] = value;
                } else {
                    transformed_row[key] = property_shape ? this.transform_result(value, property_shape) : value;
                }
            }
            return transformed_row;
        }

        if (result_shape && result_shape.kind === 'primitive') {
            if (row && typeof row === 'object' && typeof row.valueOf === 'function') {
                return this.coerce_to_primitive_type(row.valueOf(), result_shape.type);
            }
            return this.coerce_to_primitive_type(row, result_shape.type);
        }

        if (result_shape && result_shape.kind === 'value') {
            return row;
        }

        if (result_shape && result_shape.kind === 'array') {
            if (Array.isArray(row)) {
                return row.map((item: any) => this.transform_result(item, result_shape.items));
            }
            return row;
        }

        if (result_shape && result_shape.kind === 'optional') {
            if (row === undefined || row === null) {
                return undefined;
            }
            return this.transform_result(row, result_shape.shape);
        }

        return row;
    }

    private coerce_to_primitive_type(value: any, shape_type: string): any {
        if (value === undefined || value === null) {
            return value;
        }

        const bigint_types = ['Int8', 'Int16', 'Uint8', 'Uint16'];
        if (bigint_types.includes(shape_type)) {
            if (typeof value === 'bigint') {
                return value;
            }
            if (typeof value === 'number') {
                return BigInt(Math.trunc(value));
            }
            if (typeof value === 'string') {
                return BigInt(value);
            }
        }

        return value;
    }
}

function columns_to_rows(columns: Column[]): Record<string, Value>[] {
    const row_count = columns[0]?.payload.length ?? 0;
    return Array.from({length: row_count}, (_, i) => {
        const row: Record<string, Value> = {};
        for (const col of columns) {
            row[col.name] = decode({type: col.type, value: col.payload[i]});
        }
        return row;
    });
}
