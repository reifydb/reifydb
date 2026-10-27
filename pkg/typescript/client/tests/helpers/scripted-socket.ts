// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

const FRAMES = 'application/vnd.reifydb.frames';

export interface ChangeRows {
    subscriptionId: string;
    ids: number[];
}

export class ScriptedSocket {
    readyState = 1;
    readonly sent: any[] = [];
    onmessage: ((event: {data: string | ArrayBuffer}) => void) | null = null;
    onerror: ((event: unknown) => void) | null = null;
    onclose: (() => void) | null = null;
    private readonly closeListeners: (() => void)[] = [];

    addEventListener(type: string, listener: () => void): void {
        if (type === 'close') {
            this.closeListeners.push(listener);
        }
    }

    removeEventListener(): void {
    }

    send(data: string): void {
        this.sent.push(JSON.parse(data));
    }

    close(): void {
        if (this.readyState === 3) {
            return;
        }
        this.readyState = 3;
        this.onclose?.();
        this.closeListeners.forEach(listener => listener());
    }

    requests(type: string): any[] {
        return this.sent.filter(message => message.type === type);
    }

    ackSubscribe(subscriptionIds: string[]): void {
        this.ackSubscribeEntries(subscriptionIds.map((subscription_id, index) => ({index, subscription_id})));
    }

    ackSubscribeEntries(subscriptions: {index: number; subscription_id: string}[]): void {
        const request = this.requests('Subscribe').pop();
        this.receive({id: request.id, type: 'Subscribed', payload: {subscriptions}});
    }

    ackUnsubscribe(): void {
        const request = this.requests('Unsubscribe').pop();
        this.receive({id: request.id, type: 'Unsubscribed', payload: {subscription_ids: request.payload.subscription_ids}});
    }

    change(entries: ChangeRows[]): void {
        this.receive({
            type: 'Change',
            payload: {
                entries: entries.map(({subscriptionId, ids}) => ({
                    subscription_id: subscriptionId,
                    content_type: FRAMES,
                    body: {frames: [{op: 1, columns: [{name: 'id', type: {id: 'Int4'}, payload: ids.map(String)}]}]}
                }))
            }
        });
    }

    closed(subscriptionId: string): void {
        this.receive({type: 'SubscriptionClosed', payload: {subscription_id: subscriptionId}});
    }

    changeList(entries: [subscriptionId: string, rbcf: Uint8Array][]): void {
        const encoder = new TextEncoder();
        const parts: number[] = [0x01, ...u32(entries.length)];
        for (const [subscriptionId, rbcf] of entries) {
            const id = encoder.encode(subscriptionId);
            parts.push(...u32(id.length), ...id, ...u32(rbcf.length), ...rbcf);
        }
        this.onmessage?.({data: new Uint8Array(parts).buffer});
    }

    unsubscribed(): string[][] {
        return this.requests('Unsubscribe').map(request => request.payload.subscription_ids);
    }

    private receive(message: unknown): void {
        this.onmessage?.({data: JSON.stringify(message)});
    }
}

function u32(n: number): number[] {
    return [n & 0xff, (n >>> 8) & 0xff, (n >>> 16) & 0xff, (n >>> 24) & 0xff];
}
