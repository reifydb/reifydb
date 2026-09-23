// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Shape, Uuid7Value} from '@reifydb/core';
import {rql, type ReadSpec, type SpecData, type WriteSpec} from '../../src';

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

const monitor = Shape.object({id: Shape.uuid7(), name: Shape.utf8()});
const monitorRegion = Shape.object({monitor_id: Shape.uuid7(), region_id: Shape.uuid7()});

type Monitor = {id: string; name: string};
type MonitorRegion = {monitorId: string; regionId: string};

function substitutionsDoNotCompile() {
    // A ${} value is spliced into the text instead of sent as a param, so the tag must refuse it.
    const n = 5;
    // @ts-expect-error
    rql(monitor)`from uptime::monitors take ${n}`;
    // @ts-expect-error
    rql.write([monitor])`from uptime::monitors take ${n}`;
}

function dataTypeComesFromTheShape() {
    // A row type that drifts from the Shape lets a component read a field the frame never carries.
    const one = rql(monitor)`from uptime::monitors`;
    const oneIsRows: Equal<SpecData<typeof one>, Monitor[]> = true;

    const pair = rql([monitor, monitorRegion])`output from uptime::monitors; output from uptime::monitor_regions`;
    const pairIsOneArrayPerFrame: Equal<SpecData<typeof pair>, [Monitor[], MonitorRegion[]]> = true;

    const write = rql.write([monitor])`output from uptime::monitors`;
    const writeIsOneArrayPerFrame: Equal<SpecData<typeof write>, [Monitor[]]> = true;

    const none = rql.write([])`insert uptime::monitors [{ id: 1 }]`;
    const noneIsEmpty: Equal<SpecData<typeof none>, []> = true;

    const configured = rql(monitor)`from uptime::monitors`.options({config: {}});
    const optionsKeepsTheDataType: Equal<SpecData<typeof configured>, Monitor[]> = true;
}

function readAndWriteSpecsDoNotMix() {
    // A write on a read path would run a command through query rights, so the kinds must not be interchangeable.
    const read = rql([monitor])`from uptime::monitors`;
    const write = rql.write([monitor])`output from uptime::monitors`;
    // @ts-expect-error
    const readAsWrite: WriteSpec<readonly [typeof monitor], null> = read;
    // @ts-expect-error
    const writeAsRead: ReadSpec<readonly [typeof monitor], null> = write;
}

function paramsTypeStaysOnTheSpec() {
    // If the spec dropped P, a hook could not tell a wrong param key from a right one.
    const byId = rql(monitor)<{id: Uuid7Value}>`from uptime::monitors filter { id == $id }`;
    // @ts-expect-error
    const byName: ReadSpec<typeof monitor, {name: Uuid7Value}> = byId;

    const noParams = rql(monitor)`from uptime::monitors`;
    const defaultIsNull: Equal<typeof noParams, ReadSpec<typeof monitor, null>> = true;

    const configured = byId.options({config: {}});
    const optionsKeepsParams: Equal<typeof configured, ReadSpec<typeof monitor, {id: Uuid7Value}>> = true;
}

function paramsMustBeAnObjectOrNull() {
    // A scalar P cannot name param keys, so every call site would pass an unusable params value.

    // @ts-expect-error
    rql(monitor)<string>`from uptime::monitors`;
}

function writeSpecsHaveNoOptions() {
    // Config tunes a subscription or query, so a write spec that took it would silently ignore it.
    const write = rql.write([monitor])`output from uptime::monitors`;
    // @ts-expect-error
    write.options({config: {}});
}
