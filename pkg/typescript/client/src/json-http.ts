// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {
    LoginChallengeResult,
    LoginResult,
    ResponseMeta,
} from "./types";
import {
    ReifyError
} from "./types";
import {encodeParams} from "./encoder";
import {CONTENT_TYPE_JSON} from "./content-types";
import {toCamelCaseKeys, toSnakeCaseKeys, WIRE_PASSTHROUGH_KEYS} from "./case";
import {transformFrames} from "@reifydb/core";
import type {ShapeNode} from "@reifydb/core";

export interface JsonHttpClientOptions {
    url: string;
    timeoutMs?: number;
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
    async admin(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
        reqOpts?: RequestOptions
    ): Promise<any> {
        const { data } = await this.adminWithMeta(rql, params, shapes, reqOpts);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async adminWithMeta(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
        reqOpts?: RequestOptions
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute('admin', rql, params, shapes, reqOpts);
    }

    /**
     * @param rql - RQL string to execute
     */
    async command(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
        reqOpts?: RequestOptions
    ): Promise<any> {
        const { data } = await this.commandWithMeta(rql, params, shapes, reqOpts);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async commandWithMeta(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
        reqOpts?: RequestOptions
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute('command', rql, params, shapes, reqOpts);
    }

    /**
     * @param rql - RQL string to execute
     */
    async query(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
        reqOpts?: RequestOptions
    ): Promise<any> {
        const { data } = await this.queryWithMeta(rql, params, shapes, reqOpts);
        return data;
    }

    /**
     * @param rql - RQL string to execute
     */
    async queryWithMeta(
        rql: string,
        params?: any,
        shapes?: readonly ShapeNode[],
        reqOpts?: RequestOptions
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        return this.execute('query', rql, params, shapes, reqOpts);
    }

    private async execute(
        endpoint: 'admin' | 'command' | 'query',
        rql: string,
        params: any,
        shapes?: readonly ShapeNode[],
        reqOpts?: RequestOptions
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const { data, meta } = await this.send(endpoint, rql, encodedParams, reqOpts);
        return { data: transformFrames(data ?? [], shapes ?? []), meta };
    }

    private async send(
        endpoint: string,
        rql: string,
        params: any,
        reqOpts?: RequestOptions,
    ): Promise<{ data: any, meta?: ResponseMeta }> {
        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        let signal = controller.signal;
        if (reqOpts?.signal && typeof AbortSignal !== 'undefined' && 'any' in AbortSignal) {
            signal = (AbortSignal as any).any([controller.signal, reqOpts.signal]);
        } else if (reqOpts?.signal) {
            reqOpts.signal.addEventListener('abort', () => controller.abort());
        }

        const headers: Record<string, string> = {
            'Content-Type': 'application/json',
            'Accept': `${CONTENT_TYPE_JSON}, application/json`,
        };

        if (this.options.token) {
            headers['Authorization'] = `Bearer ${this.options.token}`;
        }

        const body: any = { rql };
        if (params !== undefined) {
            body.params = params;
        }

        const queryParams = new URLSearchParams({format: 'json'});
        if (this.options.unwrap) {
            queryParams.set('unwrap', 'true');
        }
        const url = `${this.options.url}/v1/${endpoint}?${queryParams}`;

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

function extractMeta(headers: Headers | undefined): ResponseMeta | undefined {
    const fingerprint = headers?.get?.('x-fingerprint');
    const duration = headers?.get?.('x-duration');
    if (!fingerprint || !duration) return undefined;
    return { fingerprint, duration };
}
