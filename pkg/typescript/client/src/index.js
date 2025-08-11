"use strict";
/**
 * MIT License
 * Copyright (c) 2025 ReifyDB
 * See license.md file for full license text
 */
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
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
exports.Schema = exports.SchemaTransformer = exports.SchemaPatterns = exports.DEFAULT_SCHEMA = exports.SchemaHelpers = exports.WsClient = exports.RowIdValue = exports.BlobValue = exports.UndefinedValue = exports.Uuid7Value = exports.Uuid4Value = exports.IntervalValue = exports.TimeValue = exports.DateTimeValue = exports.DateValue = exports.Utf8Value = exports.Float8Value = exports.Float4Value = exports.Uint16Value = exports.Uint8Value = exports.Uint4Value = exports.Uint2Value = exports.Uint1Value = exports.Int16Value = exports.Int8Value = exports.Int4Value = exports.Int2Value = exports.Int1Value = exports.BoolValue = exports.ReifyError = exports.Client = void 0;
var ws_1 = require("./ws");
var Client = /** @class */ (function () {
    function Client() {
    }
    /**
     * Connect to ReifyDB via WebSocket
     * @param url WebSocket URL
     * @param options Optional configuration
     * @returns Connected WebSocket client
     */
    Client.connect_ws = function (url_1) {
        return __awaiter(this, arguments, void 0, function (url, options) {
            if (options === void 0) { options = {}; }
            return __generator(this, function (_a) {
                return [2 /*return*/, ws_1.WsClient.connect(__assign({ url: url }, options))];
            });
        });
    };
    return Client;
}());
exports.Client = Client;
var types_1 = require("./types");
Object.defineProperty(exports, "ReifyError", { enumerable: true, get: function () { return types_1.ReifyError; } });
// Re-export core Value classes
var core_1 = require("@reifydb/core");
Object.defineProperty(exports, "BoolValue", { enumerable: true, get: function () { return core_1.BoolValue; } });
Object.defineProperty(exports, "Int1Value", { enumerable: true, get: function () { return core_1.Int1Value; } });
Object.defineProperty(exports, "Int2Value", { enumerable: true, get: function () { return core_1.Int2Value; } });
Object.defineProperty(exports, "Int4Value", { enumerable: true, get: function () { return core_1.Int4Value; } });
Object.defineProperty(exports, "Int8Value", { enumerable: true, get: function () { return core_1.Int8Value; } });
Object.defineProperty(exports, "Int16Value", { enumerable: true, get: function () { return core_1.Int16Value; } });
Object.defineProperty(exports, "Uint1Value", { enumerable: true, get: function () { return core_1.Uint1Value; } });
Object.defineProperty(exports, "Uint2Value", { enumerable: true, get: function () { return core_1.Uint2Value; } });
Object.defineProperty(exports, "Uint4Value", { enumerable: true, get: function () { return core_1.Uint4Value; } });
Object.defineProperty(exports, "Uint8Value", { enumerable: true, get: function () { return core_1.Uint8Value; } });
Object.defineProperty(exports, "Uint16Value", { enumerable: true, get: function () { return core_1.Uint16Value; } });
Object.defineProperty(exports, "Float4Value", { enumerable: true, get: function () { return core_1.Float4Value; } });
Object.defineProperty(exports, "Float8Value", { enumerable: true, get: function () { return core_1.Float8Value; } });
Object.defineProperty(exports, "Utf8Value", { enumerable: true, get: function () { return core_1.Utf8Value; } });
Object.defineProperty(exports, "DateValue", { enumerable: true, get: function () { return core_1.DateValue; } });
Object.defineProperty(exports, "DateTimeValue", { enumerable: true, get: function () { return core_1.DateTimeValue; } });
Object.defineProperty(exports, "TimeValue", { enumerable: true, get: function () { return core_1.TimeValue; } });
Object.defineProperty(exports, "IntervalValue", { enumerable: true, get: function () { return core_1.IntervalValue; } });
Object.defineProperty(exports, "Uuid4Value", { enumerable: true, get: function () { return core_1.Uuid4Value; } });
Object.defineProperty(exports, "Uuid7Value", { enumerable: true, get: function () { return core_1.Uuid7Value; } });
Object.defineProperty(exports, "UndefinedValue", { enumerable: true, get: function () { return core_1.UndefinedValue; } });
Object.defineProperty(exports, "BlobValue", { enumerable: true, get: function () { return core_1.BlobValue; } });
Object.defineProperty(exports, "RowIdValue", { enumerable: true, get: function () { return core_1.RowIdValue; } });
var ws_2 = require("./ws");
Object.defineProperty(exports, "WsClient", { enumerable: true, get: function () { return ws_2.WsClient; } });
var schema_helpers_1 = require("./schema-helpers");
Object.defineProperty(exports, "SchemaHelpers", { enumerable: true, get: function () { return schema_helpers_1.SchemaHelpers; } });
Object.defineProperty(exports, "DEFAULT_SCHEMA", { enumerable: true, get: function () { return schema_helpers_1.DEFAULT_SCHEMA; } });
// Re-export schema types from core
var core_2 = require("@reifydb/core");
Object.defineProperty(exports, "SchemaPatterns", { enumerable: true, get: function () { return core_2.SchemaPatterns; } });
Object.defineProperty(exports, "SchemaTransformer", { enumerable: true, get: function () { return core_2.SchemaTransformer; } });
// Extended Schema with additional functions
var core_3 = require("@reifydb/core");
exports.Schema = {
    // Core Schema methods
    string: core_3.Schema.string,
    number: core_3.Schema.number,
    boolean: core_3.Schema.boolean,
    bigint: core_3.Schema.bigint,
    date: core_3.Schema.date,
    undefined: core_3.Schema.undefined,
    null: core_3.Schema.null,
    boolValue: core_3.Schema.boolValue,
    int1Value: core_3.Schema.int1Value,
    int2Value: core_3.Schema.int2Value,
    int4Value: core_3.Schema.int4Value,
    int8Value: core_3.Schema.int8Value,
    int16Value: core_3.Schema.int16Value,
    uint1Value: core_3.Schema.uint1Value,
    uint2Value: core_3.Schema.uint2Value,
    uint4Value: core_3.Schema.uint4Value,
    uint8Value: core_3.Schema.uint8Value,
    uint16Value: core_3.Schema.uint16Value,
    float4Value: core_3.Schema.float4Value,
    float8Value: core_3.Schema.float8Value,
    utf8Value: core_3.Schema.utf8Value,
    dateValue: core_3.Schema.dateValue,
    dateTimeValue: core_3.Schema.dateTimeValue,
    timeValue: core_3.Schema.timeValue,
    intervalValue: core_3.Schema.intervalValue,
    uuid4Value: core_3.Schema.uuid4Value,
    uuid7Value: core_3.Schema.uuid7Value,
    undefinedValue: core_3.Schema.undefinedValue,
    blobValue: core_3.Schema.blobValue,
    rowIdValue: core_3.Schema.rowIdValue,
    object: core_3.Schema.object,
    array: core_3.Schema.array,
    tuple: core_3.Schema.tuple,
    union: core_3.Schema.union,
    optional: core_3.Schema.optional,
    auto: core_3.Schema.auto,
    bidirectional: core_3.Schema.bidirectional,
    // New convenience methods
    withPrimitiveResult: core_3.Schema.withPrimitiveResult,
    primitive: core_3.Schema.primitive,
    result: core_3.Schema.result,
    legacyParams: core_3.Schema.legacyParams
};
