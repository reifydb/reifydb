import {ErrorResponse, ReifyError, RxResponse, TxResponse} from "./types";
import {columnsToRows} from "./decoder";

let nextId = 1;

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


export class WsClient {
    private socket: WebSocket;
    private pending = new Map<string, (response: ResponsePayload) => void>();

    private constructor(socket: WebSocket) {
        this.socket = socket;

        this.socket.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            const {id, type, payload} = msg;

            const handler = this.pending.get(id);
            if (!handler) {
                console.debug(`No pending query for id: ${id}`);
                return;
            }

            this.pending.delete(id);
            handler({id, type, payload});
        };

        this.socket.onerror = (err) => {
            console.error("WebSocket error", err);
        };
    }

    static async connect(url: string): Promise<WsClient> {
        const socket = await createWebSocket(url);

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

        return new WsClient(socket);
    }

    isConnected(): boolean {
        return this.socket.readyState === this.socket.OPEN;
    }


    async tx<T extends readonly Record<string, unknown>[]>(statement: string): Promise<{
        [K in keyof T]: T[K][];
    }> {
        const id = `req-${nextId++}`;


        const message = {
            id,
            type: "Tx",
            payload: {
                statements: [statement]
            },
        };

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("ReifyDB execute timeout"));
            }, 5000);

            this.pending.set(id, (res) => {
                clearTimeout(timeout);
                resolve(res);
            });

            this.socket.send(JSON.stringify(message));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        if (response.type !== "Tx") {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        return response.payload.frames.map((frame) =>
            columnsToRows(frame.columns)
        ) as { [K in keyof T]: T[K][] };
    }

    async rx<T extends readonly Record<string, unknown>[]>(statement: string): Promise<{
        [K in keyof T]: T[K][];
    }> {
        const id = `req-${nextId++}`;

        const message = {
            id,
            type: "Rx",
            payload: {
                statements: [statement]
            },
        };

        const response = await new Promise<ResponsePayload>((resolve, reject) => {
            const timeout = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error("ReifyDB query timeout"));
            }, 5000);

            this.pending.set(id, (res) => {
                clearTimeout(timeout);
                resolve(res);
            });

            this.socket.send(JSON.stringify(message));
        });

        if (response.type === "Err") {
            throw new ReifyError(response);
        }

        if (response.type !== "Rx") {
            throw new Error(`Unexpected response type: ${response.type}`);
        }

        return response.payload.frames.map((frame) =>
            columnsToRows(frame.columns)
        ) as { [K in keyof T]: T[K][] };
    }

}
