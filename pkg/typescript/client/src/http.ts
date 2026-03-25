// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import {
    decode,
    Value
} from "@reifydb/core";
import type {
    SchemaNode,
    FrameResults,
} from "@reifydb/core";

import type {
    Column,
    LoginResult,
} from "./types";
import {
    ReifyError
} from "./types";
import {encodeParams} from "./encoder";

export interface HttpClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
}

export class HttpClient {
    private options: HttpClientOptions;

    private constructor(options: HttpClientOptions) {
        this.options = options;
    }

    static connect(options: HttpClientOptions): HttpClient {
        return new HttpClient(options);
    }

    async loginWithPassword(username: string, password: string): Promise<LoginResult> {
        return this.login("password", username, {password});
    }

    async loginWithToken(username: string, token: string): Promise<LoginResult> {
        return this.login("token", username, {token});
    }

    async login(method: string, username: string, credentials: Record<string, string>): Promise<LoginResult> {
        const timeoutMs = this.options.timeoutMs ?? 30_000;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(`${this.options.url}/v1/authenticate`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({method, username, credentials}),
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

    async admin<const S extends readonly SchemaNode[]>(
        statements: string | string[],
        params: any,
        schemas: S
    ): Promise<FrameResults<S>> {
        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const result = await this.send('admin', outputStatements, encodedParams);

        const transformedFrames = result.map((frame: any, frameIndex: number) => {
            const frameSchema = schemas[frameIndex];
            if (!frameSchema) {
                return frame;
            }
            return frame.map((row: any) => this.transformResult(row, frameSchema));
        });

        return transformedFrames as FrameResults<S>;
    }

    async command<const S extends readonly SchemaNode[]>(
        statements: string | string[],
        params: any,
        schemas: S
    ): Promise<FrameResults<S>> {
        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const result = await this.send('command', outputStatements, encodedParams);

        const transformedFrames = result.map((frame: any, frameIndex: number) => {
            const frameSchema = schemas[frameIndex];
            if (!frameSchema) {
                return frame;
            }
            return frame.map((row: any) => this.transformResult(row, frameSchema));
        });

        return transformedFrames as FrameResults<S>;
    }

    async query<const S extends readonly SchemaNode[]>(
        statements: string | string[],
        params: any,
        schemas: S
    ): Promise<FrameResults<S>> {
        const statementArray = Array.isArray(statements) ? statements : [statements];
        const outputStatements = statementArray.length > 1
            ? statementArray.map(s => s.trim() ? `OUTPUT ${s}` : s)
            : statementArray;

        const encodedParams = params !== undefined && params !== null
            ? encodeParams(params)
            : undefined;

        const result = await this.send('query', outputStatements, encodedParams);

        const transformedFrames = result.map((frame: any, frameIndex: number) => {
            const frameSchema = schemas[frameIndex];
            if (!frameSchema) {
                return frame;
            }
            return frame.map((row: any) => this.transformResult(row, frameSchema));
        });

        return transformedFrames as FrameResults<S>;
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

        try {
            const response = await fetch(`${this.options.url}/v1/${endpoint}`, {
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

            const frames = parsed.frames || [];
            return frames.map((frame: any) =>
                columnsToRows(frame.columns)
            );
        } catch (err: any) {
            clearTimeout(timeout);
            if (err.name === 'AbortError') {
                throw new Error("ReifyDB query timeout");
            }
            throw err;
        }
    }

    private transformResult(row: any, resultSchema: any): any {
        if (resultSchema && resultSchema.kind === 'object' && resultSchema.properties) {
            const transformedRow: any = {};
            for (const [key, value] of Object.entries(row)) {
                const propertySchema = resultSchema.properties[key];
                if (propertySchema && propertySchema.kind === 'primitive') {
                    if (value && typeof value === 'object' && typeof (value as any).valueOf === 'function') {
                        const rawValue = (value as any).valueOf();
                        transformedRow[key] = this.coerceToPrimitiveType(rawValue, propertySchema.type);
                    } else {
                        transformedRow[key] = this.coerceToPrimitiveType(value, propertySchema.type);
                    }
                } else if (propertySchema && propertySchema.kind === 'value') {
                    transformedRow[key] = value;
                } else {
                    transformedRow[key] = propertySchema ? this.transformResult(value, propertySchema) : value;
                }
            }
            return transformedRow;
        }

        if (resultSchema && resultSchema.kind === 'primitive') {
            if (row && typeof row === 'object' && typeof row.valueOf === 'function') {
                return this.coerceToPrimitiveType(row.valueOf(), resultSchema.type);
            }
            return this.coerceToPrimitiveType(row, resultSchema.type);
        }

        if (resultSchema && resultSchema.kind === 'value') {
            return row;
        }

        if (resultSchema && resultSchema.kind === 'array') {
            if (Array.isArray(row)) {
                return row.map((item: any) => this.transformResult(item, resultSchema.items));
            }
            return row;
        }

        if (resultSchema && resultSchema.kind === 'optional') {
            if (row === undefined || row === null) {
                return undefined;
            }
            return this.transformResult(row, resultSchema.schema);
        }

        return row;
    }

    private coerceToPrimitiveType(value: any, schemaType: string): any {
        if (value === undefined || value === null) {
            return value;
        }

        const bigintTypes = ['Int8', 'Int16', 'Uint8', 'Uint16'];
        if (bigintTypes.includes(schemaType)) {
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

function columnsToRows(columns: Column[]): Record<string, Value>[] {
    const rowCount = columns[0]?.payload.length ?? 0;
    return Array.from({length: rowCount}, (_, i) => {
        const row: Record<string, Value> = {};
        for (const col of columns) {
            row[col.name] = decode({type: col.type, value: col.payload[i]});
        }
        return row;
    });
}
