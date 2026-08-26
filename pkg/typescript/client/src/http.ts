// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {
    columns_to_rows,
    transform_frames
} from "@reifydb/core";
import type {
    ShapeNode,
    FrameResults,
} from "@reifydb/core";

import type {
    LoginChallengeResult,
    LoginResult,
    ResponseMeta,
} from "./types";
import {
    ReifyError
} from "./types";
import {encode_params} from "./encoder";
import {rbcf} from "./rbcf";
import {CONTENT_TYPE_JSON, CONTENT_TYPE_RBCF} from "./content-types";

export interface HttpClientOptions {
    url: string;
    timeout_ms?: number;
    token?: string;
    /**
     * Wire format for data frames. Defaults to `"frames"`.
     *
     * - `"json"`   — rows-shape JSON: `[[{col: val, ...}, ...], ...]`
     * - `"frames"` — frames-shape JSON: columnar frames (default)
     * - `"rbcf"`   — frames-shape binary (RBCF)
     */
    format?: "json" | "frames" | "rbcf";
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
            // Polyfill or fallback if AbortSignal.any is missing
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

    async login_challenge(method: string, credentials: Record<string, string>, req_opts?: RequestOptions): Promise<LoginChallengeResult> {
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

            if (body.status === "challenge") {
                if (!body.challenge_id || !body.payload?.message || !body.payload?.nonce) {
                    throw new Error("Malformed challenge response");
                }
                return {
                    kind: "challenge",
                    challenge_id: body.challenge_id,
                    message: body.payload.message,
                    nonce: body.payload.nonce,
                };
            }

            if (body.status === "authenticated" && body.token && body.identity) {
                this.options = {...this.options, token: body.token};
                return {kind: "authenticated", token: body.token, identity: body.identity};
            }

            throw new Error(body.reason || "Authentication failed");
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

    /**
     * @param rql - RQL string to execute
     */
    async admin<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const { frames } = await this.admin_with_meta(rql, params, shapes, req_opts);
        return frames;
    }

    /**
     * @param rql - RQL string to execute
     */
    async admin_with_meta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute('admin', rql, params, shapes, req_opts);
    }

    /**
     * @param rql - RQL string to execute
     */
    async command<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const { frames } = await this.command_with_meta(rql, params, shapes, req_opts);
        return frames;
    }

    /**
     * @param rql - RQL string to execute
     */
    async command_with_meta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute('command', rql, params, shapes, req_opts);
    }

    /**
     * @param rql - RQL string to execute
     */
    async query<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const { frames } = await this.query_with_meta(rql, params, shapes, req_opts);
        return frames;
    }

    /**
     * @param rql - RQL string to execute
     */
    async query_with_meta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute('query', rql, params, shapes, req_opts);
    }

    private async execute<const S extends readonly ShapeNode[]>(
        endpoint: 'admin' | 'command' | 'query',
        rql: string,
        params: any,
        shapes: S,
        req_opts?: RequestOptions
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        const encoded_params = params !== undefined && params !== null
            ? encode_params(params)
            : undefined;

        const { result, meta } = await this.send(endpoint, rql, encoded_params, req_opts);

        return { frames: transform_frames(result, shapes), meta };
    }

    private async send(
        endpoint: string,
        rql: string,
        params: any,
        req_opts?: RequestOptions,
    ): Promise<{ result: any, meta?: ResponseMeta }> {
        const timeout_ms = this.options.timeout_ms ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeout_ms);

        let signal = controller.signal;
        if (req_opts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, req_opts.signal]);
        } else if (req_opts?.signal) {
            req_opts.signal.addEventListener('abort', () => controller.abort());
        }

        const format = this.options.format ?? "frames";
        const headers: Record<string, string> = {
            'Content-Type': 'application/json',
            'Accept': format === "rbcf"
                ? `${CONTENT_TYPE_RBCF}, ${CONTENT_TYPE_JSON}`
                : CONTENT_TYPE_JSON,
        };

        if (this.options.token) {
            headers['Authorization'] = `Bearer ${this.options.token}`;
        }

        const body: any = { rql };
        if (params !== undefined) {
            body.params = params;
        }

        const url = `${this.options.url}/v1/${endpoint}?format=${format}`;

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

            const content_type = response.headers?.get?.('content-type') ?? '';
            const is_binary = response.ok &&
                (content_type.startsWith(CONTENT_TYPE_RBCF) || content_type.startsWith('application/octet-stream'));

            if (is_binary) {
                const buf = await response.arrayBuffer();
                const frames = rbcf.decode(new Uint8Array(buf));
                return { result: frames.map((frame: any) => columns_to_rows(frame.columns)), meta };
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

            // Response shape depends on format:
            // - "json"   → `[[{col: val}, ...], ...]` already in rows shape
            // - "frames" → `{frames: [ColumnarFrame, ...]}` needing column→row pivot
            if (format === "json") {
                return { result: parsed ?? [], meta };
            }
            const frames = parsed.frames || [];
            return {
                result: frames.map((frame: any) => columns_to_rows(frame.columns)),
                meta,
            };
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
