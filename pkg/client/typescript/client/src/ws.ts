/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {ErrorResponse, ReifyError, ReadRequest, RxResponse, WriteRequest, TxResponse, WebsocketColumn} from "./types";
import {decodeValue} from "./decoder";

type ResponsePayload = ErrorResponse | TxResponse | RxResponse;

async function createWebSocket(url: string): Promise<WebSocket> {
    if (typeof window !== "undefined" && typeof window.WebSocket !== "undefined") {
        return new WebSocket(url);
    } else {
        //@ts-ignore
        const wsModule = await import("ws");
        return new wsModule.WebSocket(url);
    }
}

export interface WsClientOptions {
    url: string;
    timeoutMs?: number;
    token?: string;
}

export class WsClient {
    private options: WsClientOptions;
    private nextId: number;
    private socket: WebSocket;
    private pending = new Map<string, (response: ResponsePayload) => void>();

    private constructor(socket: WebSocket, options: WsClientOptions) {
        this.options = options;
        this.nextId = 1;
        this.socket = socket;

        this.socket.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            const {id, type, payload} = msg;

            const handler = this.pending.get(id);
            if (!handler) {
                return;
            }

            this.pending.delete(id);
            handler({id, type, payload});
        };

        this.socket.onerror = (err) => {
            console.error("WebSocket error", err);
        };
    }

    static async connect(options: WsClientOptions): Promise<WsClient> {
        const socket = await createWebSocket(options.url);

        // Wait for connection to open if not already open
        if (socket.readyState !== socket.OPEN) {
            await new Promise<void>((resolve, reject) => {
                const onOpen = () => {
                    socket.removeEventListener("open", onOpen);
                    socket.removeEventListener("error", onError);
                    resolve();
                };

                const onError = () => {
                    socket.removeEventListener("open", onOpen);
                    socket.removeEventListener("error", onError);
                    reject(new Error("WebSocket connection failed"));
                };

                socket.addEventListener("open", onOpen);
                socket.addEventListener("error", onError);
            });
        }

        socket.send("{\"id\":\"auth-1\",\"type\":\"Auth\",\"payload\":{\"token\":\"mysecrettoken\"}}");

        return new WsClient(socket, options);
    }

    async tx<T extends readonly Record<string, unknown>[]>(statement: string): Promise<{
        [K in keyof T]: T[K][];
    }> {
        const id = `req-${this.nextId++}`;
        return await this.send({
            id,
            type: "Tx",
            payload: {
                statements: [statement]
            },
        })
    }

    async rx<T extends readonly Record<string, unknown>[]>(statement: string): Promise<{
        [K in keyof T]: T[K][];
    }> {
        const id = `req-${this.nextId++}`;
        return await this.send({
            id,
            type: "Rx",
            payload: {
                statements: [statement]
            },
        })
    }


    async send<T extends readonly Record<string, unknown>[]>(req: WriteRequest | ReadRequest): Promise<{
        [K in keyof T]: T[K][];
    }> {
        const id = req.id;

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("ReifyDB query timeout"));
            }, this.options.timeoutMs);

            this.pending.set(id, (res) => {
                clearTimeout(timeout);
                resolve(res);
            });

            this.socket.send(JSON.stringify(req));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        if (response.type !== req.type) {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        return response.payload.frames.map((frame) =>
            columnsToRows(frame.columns)
        ) as { [K in keyof T]: T[K][] };
    }

    disconnect() {
        this.socket.close();
    }
}


function columnsToRows(columns: WebsocketColumn[]): Record<string, unknown>[] {
    const rowCount = columns[0]?.data.length ?? 0;
    return Array.from({length: rowCount}, (_, i) => {
        const row: Record<string, unknown> = {};
        for (const col of columns) {
            row[col.name] = decodeValue(col.ty, col.data[i]);
        }
        return row;
    });
}
