// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import type {Params} from "@reifydb/core";
import {ReifyError} from "./types";
import {encodeParams} from "./encoder";

export interface JsonHttpClientOptions {
    url: string;
    token?: string;
    apiKey?: string;
    timeoutMs?: number;
}

export class JsonHttpClient {
    private options: JsonHttpClientOptions;

    private constructor(options: JsonHttpClientOptions) {
        this.options = options;
    }

    static connect(options: JsonHttpClientOptions): JsonHttpClient {
        return new JsonHttpClient(options);
    }

    async query<T = any>(statements: string | string[], params?: Params): Promise<T[][]> {
        return this.send<T>("query", statements, params);
    }

    async command<T = any>(statements: string | string[], params?: Params): Promise<T[][]> {
        return this.send<T>("command", statements, params);
    }

    async admin<T = any>(statements: string | string[], params?: Params): Promise<T[][]> {
        return this.send<T>("admin", statements, params);
    }

    private async send<T>(endpoint: "query" | "command" | "admin", statements: string | string[], params?: Params): Promise<T[][]> {
        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const headers: Record<string, string> = {
            "Content-Type": "application/json",
        };

        if (this.options.token) {
            headers["Authorization"] = `Bearer ${this.options.token}`;
        } else if (this.options.apiKey) {
            headers["X-Api-Key"] = this.options.apiKey;
        }

        const baseUrl = this.options.url.replace(/\/+$/, "");
        const timeoutMs = this.options.timeoutMs ?? 30_000;

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        let response: Response;
        try {
            response = await fetch(`${baseUrl}/v1/${endpoint}?format=json`, {
                method: "POST",
                headers,
                body: JSON.stringify({
                    statements: outputStatements,
                    params: encodedParams,
                }),
                signal: controller.signal,
            });
        } catch (err: any) {
            if (err.name === "AbortError") {
                throw new Error("ReifyDB query timeout");
            }
            throw err;
        } finally {
            clearTimeout(timeout);
        }

        const body = await response.json();

        if (!response.ok) {
            if (body.diagnostic) {
                throw new ReifyError({
                    id: "",
                    type: "Err",
                    payload: {diagnostic: body.diagnostic},
                });
            }
            if (body.error) {
                throw new ReifyError({
                    id: "",
                    type: "Err",
                    payload: {
                        diagnostic: {
                            code: body.code ?? "HTTP_ERROR",
                            message: body.error,
                            notes: [],
                        },
                    },
                });
            }
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        // Normalize response body to T[][]
        if (Array.isArray(body) && body.length > 0 && Array.isArray(body[0])) {
            return body as T[][];
        }

        if (Array.isArray(body)) {
            return [body] as T[][];
        }

        if (body && typeof body === "object") {
            return [[body]] as T[][];
        }

        return [] as T[][];
    }
}
