// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

const FRAMES = 'application/vnd.reifydb.frames';

export type Row = [rownum: number, id: number, name: string];

export class ScriptedSocket {
    readyState = 1;
    readonly sent: any[] = [];
    onmessage: ((event: {data: string}) => void) | null = null;
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

    ackSubscribe(subscriptionId: string): void {
        const request = this.requests('Subscribe').pop();
        this.receive({id: request.id, type: 'Subscribed', payload: {subscription_id: subscriptionId}});
    }

    ackBatch(batchId: string, subscriptionIds: (string | null)[]): void {
        const request = this.requests('BatchSubscribe').pop();
        const subscriptions = subscriptionIds
            .map((subscription_id, index) => ({index, subscription_id}))
            .filter(subscription => subscription.subscription_id !== null);
        this.receive({id: request.id, type: 'BatchSubscribed', payload: {batch_id: batchId, subscriptions}});
    }

    ackUnsubscribe(subscriptionId: string): void {
        const request = this.requests('Unsubscribe').find(sent => sent.payload.subscription_id === subscriptionId);
        this.receive({id: request.id, type: 'Unsubscribed', payload: {subscription_id: subscriptionId}});
    }

    refuse(type: string, code: string): void {
        const request = this.requests(type).pop();
        this.receive({id: request.id, type: 'Err', payload: {diagnostic: {code, message: code, notes: []}}});
    }

    insert(subscriptionId: string, rows: Row[]): void {
        const frame = {
            op: 1,
            row_numbers: rows.map(([rownum]) => String(rownum)),
            columns: [
                {name: 'id', type: {id: 'Int4'}, payload: rows.map(([, id]) => String(id))},
                {name: 'name', type: {id: 'Utf8'}, payload: rows.map(([, , name]) => name)},
            ],
        };
        this.receive({
            type: 'Change',
            payload: {subscription_id: subscriptionId, content_type: FRAMES, body: {frames: [frame]}}
        });
    }

    unsubscribed(): string[] {
        return this.requests('Unsubscribe').map(request => request.payload.subscription_id);
    }

    private receive(message: unknown): void {
        this.onmessage?.({data: JSON.stringify(message)});
    }
}
