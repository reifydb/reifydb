"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.WsClient = void 0;
/*
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
var core_1 = require("@reifydb/core");
var types_1 = require("./types");
function createWebSocket(url) {
    return __awaiter(this, void 0, void 0, function () {
        var wsModule;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (!(typeof window !== "undefined" && typeof window.WebSocket !== "undefined")) return [3 /*break*/, 1];
                    return [2 /*return*/, new WebSocket(url)];
                case 1: return [4 /*yield*/, Promise.resolve().then(function () { return require("ws"); })];
                case 2:
                    wsModule = _a.sent();
                    return [2 /*return*/, new wsModule.WebSocket(url)];
            }
        });
    });
}
var WsClient = /** @class */ (function () {
    function WsClient(socket, options) {
        var _this = this;
        this.pending = new Map();
        this.options = options;
        this.nextId = 1;
        this.socket = socket;
        this.socket.onmessage = function (event) {
            var msg = JSON.parse(event.data);
            var id = msg.id, type = msg.type, payload = msg.payload;
            var handler = _this.pending.get(id);
            if (!handler) {
                return;
            }
            _this.pending.delete(id);
            handler({ id: id, type: type, payload: payload });
        };
        this.socket.onerror = function (err) {
            console.error("WebSocket error", err);
        };
    }
    WsClient.connect = function (options) {
        return __awaiter(this, void 0, void 0, function () {
            var socket;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, createWebSocket(options.url)];
                    case 1:
                        socket = _a.sent();
                        if (!(socket.readyState !== socket.OPEN)) return [3 /*break*/, 3];
                        return [4 /*yield*/, new Promise(function (resolve, reject) {
                                var onOpen = function () {
                                    socket.removeEventListener("open", onOpen);
                                    socket.removeEventListener("error", onError);
                                    resolve();
                                };
                                var onError = function () {
                                    socket.removeEventListener("open", onOpen);
                                    socket.removeEventListener("error", onError);
                                    reject(new Error("WebSocket connection failed"));
                                };
                                socket.addEventListener("open", onOpen);
                                socket.addEventListener("error", onError);
                            })];
                    case 2:
                        _a.sent();
                        _a.label = 3;
                    case 3:
                        socket.send("{\"id\":\"reifydb-auth-1\",\"type\":\"Auth\",\"payload\":{\"token\":\"mysecrettoken\"}}");
                        return [2 /*return*/, new WsClient(socket, options)];
                }
            });
        });
    };
    WsClient.prototype.command = function (statement, params, schemas) {
        return __awaiter(this, void 0, void 0, function () {
            var id, encodedParams, result, transformedFrames;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        id = "req-".concat(this.nextId++);
                        encodedParams = this.encodeWithSchema(params, null);
                        return [4 /*yield*/, this.send({
                                id: id,
                                type: "Command",
                                payload: {
                                    statements: [statement],
                                    params: encodedParams
                                },
                            })];
                    case 1:
                        result = _a.sent();
                        transformedFrames = result.map(function (frame, frameIndex) {
                            var frameSchema = schemas[frameIndex];
                            if (!frameSchema) {
                                return frame; // No schema for this frame, return as-is
                            }
                            return frame.map(function (row) { return _this.transformResult(row, frameSchema); });
                        });
                        return [2 /*return*/, transformedFrames];
                }
            });
        });
    };
    WsClient.prototype.query = function (statement, paramsOrSchema, schema) {
        return __awaiter(this, void 0, void 0, function () {
            var id, actualParams, actualSchema, resultSchema, encodedParams, result;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        id = "req-".concat(this.nextId++);
                        actualParams = undefined;
                        // Handle overloads: (statement, params, schema) or (statement, schema)
                        if (schema) {
                            actualParams = paramsOrSchema;
                            actualSchema = schema;
                        }
                        else {
                            actualSchema = paramsOrSchema;
                        }
                        resultSchema = null;
                        if (actualSchema) {
                            if ('result' in actualSchema) {
                                // BidirectionalSchema
                                resultSchema = actualSchema.result;
                            }
                            else if ('kind' in actualSchema) {
                                // Raw schema node
                                resultSchema = actualSchema;
                            }
                        }
                        encodedParams = undefined;
                        if (actualParams !== undefined) {
                            if (actualSchema && actualSchema.params) {
                                encodedParams = this.encodeWithSchema(actualParams, actualSchema.params);
                            }
                            else {
                                // Fallback encoding for raw schema nodes
                                encodedParams = this.encodeWithSchema(actualParams, null);
                            }
                        }
                        return [4 /*yield*/, this.send({
                                id: id,
                                type: "Query",
                                payload: {
                                    statements: [statement],
                                    params: encodedParams
                                },
                            })];
                    case 1:
                        result = _a.sent();
                        // Decode results if schema provided
                        if (resultSchema) {
                            return [2 /*return*/, result.map(function (frame) {
                                    return frame.map(function (row) { return _this.transformResult(row, resultSchema); });
                                })];
                        }
                        return [2 /*return*/, result];
                }
            });
        });
    };
    WsClient.prototype.send = function (req) {
        return __awaiter(this, void 0, void 0, function () {
            var id, response;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        id = req.id;
                        return [4 /*yield*/, new Promise(function (resolve, reject) {
                                var timeout = setTimeout(function () {
                                    _this.pending.delete(id);
                                    reject(new Error("ReifyDB query timeout"));
                                }, _this.options.timeoutMs);
                                _this.pending.set(id, function (res) {
                                    clearTimeout(timeout);
                                    resolve(res);
                                });
                                _this.socket.send(JSON.stringify(req));
                            })];
                    case 1:
                        response = _a.sent();
                        if (response.type === "Err") {
                            throw new types_1.ReifyError(response);
                        }
                        if (response.type !== req.type) {
                            throw new Error("Unexpected response type: ".concat(response.type));
                        }
                        return [2 /*return*/, response.payload.frames.map(function (frame) {
                                return columnsToRows(frame.columns);
                            })];
                }
            });
        });
    };
    WsClient.prototype.encodeWithSchema = function (params, schema) {
        var _this = this;
        // For LEGACY_SCHEMA-like usage, if we have Value objects, encode them directly
        if (this.isValueObjectParams(params)) {
            return this.encodeValueObjectParams(params);
        }
        // If no schema provided, use fallback encoding
        if (!schema) {
            return this.encodePrimitiveParams(params);
        }
        // For primitive parameters with LEGACY_SCHEMA, encode directly using fallback
        if (this.isLegacySchema(schema)) {
            return this.encodePrimitiveParams(params);
        }
        var encodedParams = core_1.SchemaTransformer.encodeParams(params, schema);
        // Convert the schema-encoded result to the expected Params format
        if (Array.isArray(encodedParams)) {
            return encodedParams.map(function (param) {
                if (param && typeof param === 'object' && 'encode' in param) {
                    return param.encode();
                }
                // Fallback encoding for primitives
                return _this.fallbackEncode(param);
            });
        }
        else {
            var encoded = {};
            for (var _i = 0, _a = Object.entries(encodedParams); _i < _a.length; _i++) {
                var _b = _a[_i], key = _b[0], value = _b[1];
                if (value && typeof value === 'object' && 'encode' in value) {
                    encoded[key] = value.encode();
                }
                else {
                    encoded[key] = this.fallbackEncode(value);
                }
            }
            return encoded;
        }
    };
    WsClient.prototype.isValueObjectParams = function (params) {
        if (!params || typeof params !== 'object') {
            return false;
        }
        if (Array.isArray(params)) {
            return params.some(function (p) { return p && typeof p === 'object' && 'encode' in p; });
        }
        return Object.values(params).some(function (v) { return v && typeof v === 'object' && 'encode' in v; });
    };
    WsClient.prototype.encodeValueObjectParams = function (params) {
        var _this = this;
        if (Array.isArray(params)) {
            return params.map(function (param) {
                if (param && typeof param === 'object' && 'encode' in param) {
                    return param.encode();
                }
                return _this.fallbackEncode(param);
            });
        }
        else {
            var encoded = {};
            for (var _i = 0, _a = Object.entries(params); _i < _a.length; _i++) {
                var _b = _a[_i], key = _b[0], value = _b[1];
                if (value && typeof value === 'object' && 'encode' in value) {
                    encoded[key] = value.encode();
                }
                else {
                    encoded[key] = this.fallbackEncode(value);
                }
            }
            return encoded;
        }
    };
    WsClient.prototype.isLegacySchema = function (schema) {
        // Check if this is the LEGACY_SCHEMA by looking for its characteristic structure
        // LEGACY_SCHEMA has params: Schema.optional(Schema.union(...)) and no result schema
        return schema &&
            schema.kind === 'optional' &&
            schema.schema &&
            schema.schema.kind === 'union';
    };
    WsClient.prototype.encodePrimitiveParams = function (params) {
        var _this = this;
        if (Array.isArray(params)) {
            return params.map(function (param) { return _this.fallbackEncode(param); });
        }
        else {
            var encoded = {};
            for (var _i = 0, _a = Object.entries(params); _i < _a.length; _i++) {
                var _b = _a[_i], key = _b[0], value = _b[1];
                encoded[key] = this.fallbackEncode(value);
            }
            return encoded;
        }
    };
    WsClient.prototype.transformResult = function (row, resultSchema) {
        console.log('transformResult called with:', { row: row, resultSchema: resultSchema });
        // Handle primitive schema transformation
        if (resultSchema && resultSchema.kind === 'primitive') {
            console.log('Using primitive branch');
            var transformedRow = {};
            for (var _i = 0, _a = Object.entries(row); _i < _a.length; _i++) {
                var _b = _a[_i], key = _b[0], value = _b[1];
                // If it's a Value object with .value property, extract the primitive
                if (value && typeof value === 'object' && 'value' in value) {
                    transformedRow[key] = value.value;
                }
                else {
                    transformedRow[key] = value;
                }
            }
            console.log('Transformed row:', transformedRow);
            return transformedRow;
        }
        // Handle union schema - check if first type is an object with primitive properties
        if (resultSchema && resultSchema.kind === 'union' && resultSchema.types && resultSchema.types.length > 0) {
            var firstType = resultSchema.types[0];
            if (firstType && firstType.kind === 'object' && firstType.properties) {
                console.log('Using union schema with object primitive conversion');
                var transformedRow = {};
                for (var _c = 0, _d = Object.entries(row); _c < _d.length; _c++) {
                    var _e = _d[_c], key = _e[0], value = _e[1];
                    var propertySchema = firstType.properties[key];
                    if (propertySchema && propertySchema.kind === 'primitive') {
                        // Convert Value objects to primitives for primitive schema properties
                        if (value && typeof value === 'object' && 'value' in value) {
                            transformedRow[key] = value.value;
                        }
                        else {
                            transformedRow[key] = value;
                        }
                    }
                    else {
                        // Keep as-is for non-primitive properties
                        transformedRow[key] = value;
                    }
                }
                console.log('Transformed row:', transformedRow);
                return transformedRow;
            }
        }
        // Handle object schema with primitive properties - extract primitives from Value objects
        if (resultSchema && resultSchema.kind === 'object' && resultSchema.properties) {
            console.log('Using object schema with primitive conversion');
            var transformedRow = {};
            for (var _f = 0, _g = Object.entries(row); _f < _g.length; _f++) {
                var _h = _g[_f], key = _h[0], value = _h[1];
                var propertySchema = resultSchema.properties[key];
                if (propertySchema && propertySchema.kind === 'primitive') {
                    // Convert Value objects to primitives for primitive schema properties
                    if (value && typeof value === 'object' && 'value' in value) {
                        transformedRow[key] = value.value;
                    }
                    else {
                        transformedRow[key] = value;
                    }
                }
                else {
                    // Keep as-is for non-primitive properties
                    transformedRow[key] = value;
                }
            }
            console.log('Transformed row:', transformedRow);
            return transformedRow;
        }
        // Default to using SchemaTransformer
        console.log('Using SchemaTransformer.decodeResult');
        var result = core_1.SchemaTransformer.decodeResult(row, resultSchema);
        console.log('SchemaTransformer.decodeResult returned:', result);
        return result;
    };
    WsClient.prototype.fallbackEncode = function (value) {
        if (value === null || value === undefined) {
            return { type: 'Undefined', value: '⟪undefined⟫' };
        }
        switch (typeof value) {
            case 'boolean':
                return { type: 'Bool', value: value.toString() };
            case 'number':
                if (Number.isInteger(value)) {
                    if (value >= -128 && value <= 127) {
                        return { type: 'Int1', value: value.toString() };
                    }
                    else if (value >= -32768 && value <= 32767) {
                        return { type: 'Int2', value: value.toString() };
                    }
                    else if (value >= -2147483648 && value <= 2147483647) {
                        return { type: 'Int4', value: value.toString() };
                    }
                    else {
                        return { type: 'Int8', value: value.toString() };
                    }
                }
                else {
                    return { type: 'Float8', value: value.toString() };
                }
            case 'string':
                return { type: 'Utf8', value: value };
            case 'bigint':
                return { type: 'Int8', value: value.toString() };
            default:
                if (value instanceof Date) {
                    return { type: 'DateTime', value: value.toISOString() };
                }
                throw new Error("Unsupported parameter type: ".concat(typeof value));
        }
    };
    WsClient.prototype.disconnect = function () {
        this.socket.close();
    };
    return WsClient;
}());
exports.WsClient = WsClient;
function columnsToRows(columns) {
    var _a, _b;
    var rowCount = (_b = (_a = columns[0]) === null || _a === void 0 ? void 0 : _a.data.length) !== null && _b !== void 0 ? _b : 0;
    return Array.from({ length: rowCount }, function (_, i) {
        var row = {};
        for (var _i = 0, columns_1 = columns; _i < columns_1.length; _i++) {
            var col = columns_1[_i];
            row[col.name] = (0, core_1.decode)({ type: col.ty, value: col.data[i] });
        }
        return row;
    });
}
