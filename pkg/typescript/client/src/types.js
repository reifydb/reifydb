"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.ReifyError = void 0;
var ReifyError = /** @class */ (function (_super) {
    __extends(ReifyError, _super);
    function ReifyError(response) {
        var _newTarget = this.constructor;
        var _this = this;
        var _a;
        var diagnostic = response.payload.diagnostic;
        var message = "[".concat(diagnostic.code, "] ").concat(diagnostic.message) +
            (diagnostic.label ? " \u2014 ".concat(diagnostic.label) : "");
        _this = _super.call(this, message) || this;
        _this.name = "ReifyError";
        _this.code = diagnostic.code;
        _this.statement = diagnostic.statement;
        _this.column = diagnostic.column;
        _this.span = diagnostic.span;
        _this.label = diagnostic.label;
        _this.help = diagnostic.help;
        _this.notes = (_a = diagnostic.notes) !== null && _a !== void 0 ? _a : [];
        _this.cause = diagnostic.cause;
        // Required for instanceof checks to work properly
        Object.setPrototypeOf(_this, _newTarget.prototype);
        return _this;
    }
    ReifyError.prototype.toString = function () {
        var position = this.span
            ? "line ".concat(this.span.line, ", offset ").concat(this.span.offset)
            : "unknown position";
        var notes = this.notes.length
            ? "\nNotes:\n- ".concat(this.notes.join("\n- "))
            : "";
        var help = this.help
            ? "\nHelp: ".concat(this.help)
            : "";
        return "".concat(this.name, ": ").concat(this.message, "\nAt ").concat(position).concat(help).concat(notes);
    };
    return ReifyError;
}(Error));
exports.ReifyError = ReifyError;
