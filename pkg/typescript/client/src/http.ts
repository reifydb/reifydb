// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {
    columnsToRows,
    transformFrames
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
import {encodeParams} from "./encoder";
import {rbcf} from "./rbcf";
import {CONTENT_TYPE_JSON, CONTENT_TYPE_RBCF} from "./content-types";
import {toCamelCaseKeys, toSnakeCaseKeys, WIRE_PASSTHROUGH_KEYS} from "./case";

export interface HttpClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
    /**
     * Wire format for data frames. Defaults to `"frames"`.
     *
     * - `"json"`   - rows-shape JSON: `[[{col: val, ...}, ...], ...]`
     * - `"frames"` - frames-shape JSON: columnar frames (default)
     * - `"rbcf"`   - frames-shape binary (RBCF)
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

    async loginWithPassword(identity: string, password: string, reqOpts?: RequestOptions): Promise<LoginResult> {
        return this.login("password", {identifier: identity, password}, reqOpts);
    }

    async loginWithToken(token: string, reqOpts?: RequestOptions): Promise<LoginResult> {
        return this.login("token", {token}, reqOpts);
    }

    async login(method: string, credentials: Record<string, string>, reqOpts?: RequestOptions): Promise<LoginResult> {
        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        let signal = controller.signal;
        if (reqOpts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, reqOpts.signal]);
        } else if (reqOpts?.signal) {
            reqOpts.signal.addEventListener('abort', () => controller.abort());
        }

        try {
            const response = await fetch(`${this.options.url}/v1/authenticate`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(toSnakeCaseKeys({method, credentials}, WIRE_PASSTHROUGH_KEYS)),
                signal,
            });

            clearTimeout(timeout);
            const body = toCamelCaseKeys<any>(await response.json(), WIRE_PASSTHROUGH_KEYS);

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

    async loginChallenge(method: string, credentials: Record<string, string>, reqOpts?: RequestOptions): Promise<LoginChallengeResult> {
        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        let signal = controller.signal;
        if (reqOpts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, reqOpts.signal]);
        } else if (reqOpts?.signal) {
            reqOpts.signal.addEventListener('abort', () => controller.abort());
        }

        try {
            const response = await fetch(`${this.options.url}/v1/authenticate`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(toSnakeCaseKeys({method, credentials}, WIRE_PASSTHROUGH_KEYS)),
                signal,
            });

            clearTimeout(timeout);
            const body = toCamelCaseKeys<any>(await response.json(), WIRE_PASSTHROUGH_KEYS);

            if (body.status === "challenge") {
                if (!body.challengeId || !body.payload?.message || !body.payload?.nonce) {
                    throw new Error("Malformed challenge response");
                }
                return {
                    kind: "challenge",
                    challengeId: body.challengeId,
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

    async logout(reqOpts?: RequestOptions): Promise<void> {
        if (!this.options.token) {
            return;
        }

        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        let signal = controller.signal;
        if (reqOpts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, reqOpts.signal]);
        } else if (reqOpts?.signal) {
            reqOpts.signal.addEventListener('abort', () => controller.abort());
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
        reqOpts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const { frames } = await this.adminWithMeta(rql, params, shapes, reqOpts);
        return frames;
    }

    /**
     * @param rql - RQL string to execute
     */
    async adminWithMeta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        reqOpts?: RequestOptions
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute('admin', rql, params, shapes, reqOpts);
    }

    /**
     * @param rql - RQL string to execute
     */
    async command<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        reqOpts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const { frames } = await this.commandWithMeta(rql, params, shapes, reqOpts);
        return frames;
    }

    /**
     * @param rql - RQL string to execute
     */
    async commandWithMeta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        reqOpts?: RequestOptions
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute('command', rql, params, shapes, reqOpts);
    }

    /**
     * @param rql - RQL string to execute
     */
    async query<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        reqOpts?: RequestOptions
    ): Promise<FrameResults<S>> {
        const { frames } = await this.queryWithMeta(rql, params, shapes, reqOpts);
        return frames;
    }

    /**
     * @param rql - RQL string to execute
     */
    async queryWithMeta<const S extends readonly ShapeNode[]>(
        rql: string,
        params: any,
        shapes: S,
        reqOpts?: RequestOptions
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        return this.execute('query', rql, params, shapes, reqOpts);
    }

    private async execute<const S extends readonly ShapeNode[]>(
        endpoint: 'admin' | 'command' | 'query',
        rql: string,
        params: any,
        shapes: S,
        reqOpts?: RequestOptions
    ): Promise<{ frames: FrameResults<S>, meta?: ResponseMeta }> {
        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const { result, meta } = await this.send(endpoint, rql, encodedParams, reqOpts);

        return { frames: transformFrames(result, shapes), meta };
    }

    private async send(
        endpoint: string,
        rql: string,
        params: any,
        reqOpts?: RequestOptions,
    ): Promise<{ result: any, meta?: ResponseMeta }> {
        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        let signal = controller.signal;
        if (reqOpts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, reqOpts.signal]);
        } else if (reqOpts?.signal) {
            reqOpts.signal.addEventListener('abort', () => controller.abort());
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
                body: JSON.stringify(toSnakeCaseKeys(body, WIRE_PASSTHROUGH_KEYS)),
                signal,
                credentials: 'include',
            });

            clearTimeout(timeout);

            const meta = extractMeta(response.headers);

            const contentType = response.headers?.get?.('content-type') ?? '';
            const isBinary = response.ok &&
                (contentType.startsWith(CONTENT_TYPE_RBCF) || contentType.startsWith('application/octet-stream'));

            if (isBinary) {
                const buf = await response.arrayBuffer();
                const frames = rbcf.decode(new Uint8Array(buf));
                return { result: frames.map((frame: any) => columnsToRows(frame.columns)), meta };
            }

            const responseBody = await response.text();
            let parsed: any;
            try {
                parsed = JSON.parse(responseBody);
            } catch {
                throw new Error(`Invalid JSON response: ${responseBody}`);
            }

            if (!response.ok) {
                const errBody = toCamelCaseKeys<any>(parsed, WIRE_PASSTHROUGH_KEYS);
                if (errBody.diagnostic) {
                    throw new ReifyError({
                        id: '',
                        type: 'Err',
                        payload: {diagnostic: errBody.diagnostic}
                    });
                }
                throw new Error(errBody.error || `HTTP ${response.status}: ${responseBody}`);
            }

            if (format === "json") {
                return { result: parsed ?? [], meta };
            }
            const frames = parsed.frames || [];
            return {
                result: frames.map((frame: any) => columnsToRows(frame.columns)),
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

function extractMeta(headers: Headers | undefined): ResponseMeta | undefined {
    const fingerprint = headers?.get?.('x-fingerprint');
    const duration = headers?.get?.('x-duration');
    if (!fingerprint || !duration) return undefined;
    return { fingerprint, duration };
}
