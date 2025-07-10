/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {WsClient, WsClientOptions} from "./ws";

export class Client {
    /**
     * Connect to ReifyDB via WebSocket
     * @param url WebSocket URL
     * @param options Optional configuration
     * @returns Connected WebSocket client
     */
    static async connect_ws(url: string, options: Omit<WsClientOptions, 'url'> = {}): Promise<WsClient> {
        return WsClient.connect({url, ...options});
    }

}


async function main() {
    const client = await Client.connect_ws("ws://127.0.0.1:9001");

    const frames = await client.tx<[
        { abc: number },
        { dec: number }
    ]>("SELECT cast(127, int1) as abc; SELECT 2 as dec;");

    const frame0 = frames[0];
    const frame1 = frames[1];

    console.log(frame0[0].abc);
    console.log(frame1[0].dec);
}


main();