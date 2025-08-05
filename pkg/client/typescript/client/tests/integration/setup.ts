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
            {encoding: 'utf8'}
        );
        return result.trim().length > 0;
    } catch {
        return false;
    }
}

async function startContainer(): Promise<void> {
    execSync(`docker compose -f ${COMPOSE_FILE} up -d ${SERVICE_NAME}`, {stdio: 'inherit'});
    await new Promise(resolve => setTimeout(resolve, 2000));
}


export default async function setup() {
    if (!process.env.CI && !isContainerRunning()) {
        console.info('Starting test container...');
        await startContainer();
        console.info('Test container started successfully');
    }
}


export async function waitForDatabase(maxRetries = 30, delay = 1000): Promise<void> {
    for (let i = 0; i < maxRetries; i++) {
        let url = process.env.REIFYDB_WS_URL;
        let client = null;
        try {
            client = await Client.connect_ws(url, {timeoutMs: 5000});
            // await client.query('MAP 1;');
            return;
        } catch (error) {
            console.log(`❌ Database connection failed on attempt ${i + 1}: ${error.message}`);
            if (i === maxRetries - 1) {
                throw new Error(`${url} not ready after ${maxRetries} attempts`);
            }
            await new Promise(resolve => setTimeout(resolve, delay));
        } finally {
            if (client) {
                try {
                    client.disconnect();
                } catch (e) {
                    // Ignore disconnect errors
                }
            }
        }
    }
}