/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */

import { Schema, InferSchema } from '.';

console.log('Testing basic schema functionality...\n');

const boolSchema = Schema.bool();
console.log('Bool schema:', boolSchema);

const stringSchema = Schema.string();
console.log('String schema:', stringSchema);

const objectSchema = Schema.object({
    id: Schema.int4(),
    name: Schema.string(),
    active: Schema.bool()
});
console.log('Object schema:', objectSchema);

type TestObject = InferSchema<typeof objectSchema>;

const testObj: TestObject = {
    id: 123,
    name: "test",
    active: true
};

console.log('\nTest object:', testObj);
console.log('Type checking successful!');

console.log('\n✅ Basic schema test passed!');