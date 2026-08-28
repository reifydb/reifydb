// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Base58 alphabet used by Solana (Bitcoin's flavor, not the Flickr variant).
const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const ALPHABET_SET = new Set(ALPHABET);

export function encodeBase58(bytes: Uint8Array): string {
  if (bytes.length === 0) return "";

  // Count leading zero bytes; they become leading '1's in base58.
  let leadingZeros = 0;
  while (leadingZeros < bytes.length && bytes[leadingZeros] === 0) {
    leadingZeros += 1;
  }

  // Convert the rest to base58 by repeated division.
  const digits: number[] = [];
  for (let i = leadingZeros; i < bytes.length; i += 1) {
    let carry = bytes[i];
    for (let j = 0; j < digits.length; j += 1) {
      carry += digits[j] * 256;
      digits[j] = carry % 58;
      carry = Math.floor(carry / 58);
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = Math.floor(carry / 58);
    }
  }

  let out = "";
  for (let i = 0; i < leadingZeros; i += 1) out += ALPHABET[0];
  for (let i = digits.length - 1; i >= 0; i -= 1) out += ALPHABET[digits[i]];
  return out;
}

export function isBase58(value: string): boolean {
  if (value.length === 0) return false;
  for (let i = 0; i < value.length; i += 1) {
    if (!ALPHABET_SET.has(value[i])) return false;
  }
  return true;
}
