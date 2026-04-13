// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { decode } from "./decode";
import { encode } from "./encode";

export type { WireFrame, WireColumn } from "./types";
export { RBCF_MAGIC, RBCF_VERSION } from "./format";

export const rbcf = {
    encode,
    decode,
};
