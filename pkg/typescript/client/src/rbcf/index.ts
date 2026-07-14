// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { decode } from "./decode";
import { encode } from "./encode";

export type { WireFrame, WireColumn } from "./types";
export { RBCF_MAGIC, RBCF_VERSION, TYPE_CODE, type_name_from_code, type_name_from_tag } from "./format";
export type { TypeName } from "./format";

export const rbcf = {
    encode,
    decode,
};
