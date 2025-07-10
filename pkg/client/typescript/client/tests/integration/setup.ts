/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import {execSync} from 'child_process';
import {Client} from "../../src";

const COMPOSE_FILE = 'tests/docker-compose.yml';
const SERVICE_NAME = 'reifydb-test';

function isContainerRunning(): boolean {
    try {
        const result = execSync(
            `docker compose -f ${COMPOSE_FILE} ps -q ${SERVICE_NAME}`,
            {encoding: 'utf8', stdio: 'inherit'}
        );
        return result.trim().length > 0;
    } catch {
        return false;
    }
}

async function startContainer(): Promise<void> {
    execSync(`docker compose -f ${COMPOSE_FILE} restart`, {stdio: 'inherit'});
    await new Promise(resolve => setTimeout(resolve, 2000));
}


export default async function setup() {
    if (isContainerRunning()) {
        console.info('Starting test database server...');
        await startContainer();
        console.info('Test database server started successfully');
    }
}


export async function waitForDatabase(maxRetries = 30, delay = 1000): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
        try {
            const client = await Client.connect_ws(
                process.env.REIFYDB_WS_URL || 'ws://127.0.0.1:9090',
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