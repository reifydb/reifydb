// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

// Base58 alphabet used by Solana (Bitcoin's flavor, not the Flickr variant).
const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const ALPHABET_SET = new Set(ALPHABET);

export function encode_base58(bytes: Uint8Array): string {
  if (bytes.length === 0) return "";

  // Count leading zero bytes; they become leading '1's in base58.
  let leading_zeros = 0;
  while (leading_zeros < bytes.length && bytes[leading_zeros] === 0) {
    leading_zeros += 1;
  }

  // Convert the rest to base58 by repeated division.
  const digits: number[] = [];
  for (let i = leading_zeros; i < bytes.length; i += 1) {
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
  for (let i = 0; i < leading_zeros; i += 1) out += ALPHABET[0];
  for (let i = digits.length - 1; i >= 0; i -= 1) out += ALPHABET[digits[i]];
  return out;
}

export function is_base58(value: string): boolean {
  if (value.length === 0) return false;
  for (let i = 0; i < value.length; i += 1) {
    if (!ALPHABET_SET.has(value[i])) return false;
  }
  return true;
}
