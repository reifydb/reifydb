// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

import { decode } from "./decode";
import { encode } from "./encode";

export type { WireFrame, WireColumn } from "./types";
export { RBCF_MAGIC, RBCF_VERSION } from "./format";

export const rbcf = {
    encode,
    decode,
};
