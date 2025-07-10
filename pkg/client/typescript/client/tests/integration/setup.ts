/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {ChildProcess, spawn} from 'child_process';
import {join} from 'path';
import {Client} from "../../src";

let testServer: TestDatabaseServer;

export default async function setup() {
    console.info('Starting test database server...');

    // testServer = new TestDatabaseServer({
    //     port: parseInt(process.env.REIFYDB_TEST_PORT || '9001'),
    //     dbPath: process.env.REIFYDB_TEST_DB_PATH || './test-data'
    // });
    //
    // await testServer.start();
    //
    // // Wait for server to be ready
    // // await testServer.waitForReady();
    //
    // // Store server reference for teardown
    // (global as any).__TEST_SERVER__ = testServer;

    console.info('Test database server started successfully');
}


export class TestDatabaseServer {
    private process: ChildProcess | null = null;
    private readonly port: number;
    private readonly dbPath: string;

    constructor(port = 9001) {
        this.port = port;
        this.dbPath = join(__dirname, 'test-db');
    }

    async start(): Promise<void> {
        return new Promise((resolve, reject) => {
            // Start your ReifyDB server for testing
            this.process = spawn('reifydb-server', [
                '--port', this.port.toString(),
                '--db-path', this.dbPath,
                '--test-mode'
            ]);

            this.process.on('error', reject);

            // Wait for server to be ready
            setTimeout(() => {
                if (this.process && !this.process.killed) {
                    resolve();
                } else {
                    reject(new Error('Failed to start test database server'));
                }
            }, 2000);
        });
    }

    async stop(): Promise<void> {
        if (this.process) {
            this.process.kill();
            this.process = null;
        }
    }
}

export async function waitForDatabase(maxRetries = 30, delay = 1000): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
        try {
            const client = await Client.connect_ws(
                process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:9001',
                {timeoutMs: 2000}
            );

            await client.tx('SELECT 1;');
            await client.disconnect();
            return;
        } catch (error) {
            if (i === maxRetries - 1) {
                throw new Error(`Database not ready after ${maxRetries} attempts`);
            }
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}