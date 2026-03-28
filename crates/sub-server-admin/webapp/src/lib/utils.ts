// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}