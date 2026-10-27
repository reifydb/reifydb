// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll, describe, expect, it } from "vitest";
import { rbcf } from "../src/rbcf";
import * as path from "path";
import * as fs from "fs";
import init, { encode_rbcf_fixture } from "../../wasm/wasm/reifydb_webassembly.js";
import { wasmBytes } from "../../wasm/src/load";

describe("RBCF Conformance", () => {
    beforeAll(async () => {
        await init({ module_or_path: await wasmBytes() });
    }, 30_000);

    const conformanceRoot = path.resolve(__dirname, "../../../test/conformance/wire-format");

    if (!fs.existsSync(conformanceRoot)) {
        throw new Error(`Conformance directory not found at ${conformanceRoot}`);
    }

    const encodings = fs.readdirSync(conformanceRoot).filter(f => 
        fs.statSync(path.join(conformanceRoot, f)).isDirectory()
    );

    for (const encoding of encodings) {
        describe(`Encoding: ${encoding}`, () => {
            const encodingDir = path.join(conformanceRoot, encoding);
            const typeFiles = fs.readdirSync(encodingDir).filter(f => f.endsWith(".json"));

            for (const typeFile of typeFiles) {
                const typeName = path.basename(typeFile, ".json");
                
                it(`should correctly decode ${typeName} with ${encoding} encoding`, () => {
                    const filePath = path.join(encodingDir, typeFile);
                    const cases = JSON.parse(fs.readFileSync(filePath, "utf-8"));

                    for (const [index, testCase] of cases.entries()) {
                        const framesJson = JSON.stringify(testCase.frames);
                        
                        // 1. Get golden bytes from Rust implementation via WASM
                        const goldenBytes = encode_rbcf_fixture(framesJson, encoding);
                        
                        // 2. Decode using TypeScript implementation
                        const decodedFrames = rbcf.decode(goldenBytes);

                        // 3. Assert equality
                        // Normalize frames for comparison (handle row_numbers stringification, empty metadata arrays, and float precision)
                        const normalize = (frames: any[]) => frames.map((f: any) => ({
                            columns: f.columns.map((c: any) => {
                                if (c.type === "Float4" || c.type?.Option === "Float4") {
                                    return {
                                        ...c,
                                        payload: c.payload.map((v: string) => v === "⟪none⟫" ? v : Math.fround(Number(v)))
                                    };
                                }
                                if (c.type === "Float8" || c.type?.Option === "Float8") {
                                    return {
                                        ...c,
                                        payload: c.payload.map((v: string) => v === "⟪none⟫" ? v : Number(v))
                                    };
                                }
                                return c;
                            }),
                            row_numbers: (f.row_numbers || []).map((n: any) => String(n)),
                            created_at: f.created_at || [],
                            updated_at: f.updated_at || [],
                        }));

                        const normalizedExpected = normalize(testCase.frames);
                        const normalizedDecoded = normalize(decodedFrames);

                        try {
                            expect(normalizedDecoded).toEqual(normalizedExpected);
                        } catch (e) {
                            console.error(`Mismatch in ${encoding}/${typeFile} case ${index}`);
                            throw e;
                        }
                    }
                });
            }
        });
    }
});
