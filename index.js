"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.initStorage = void 0;
const minio_1 = __importDefault(require("./libs/minio"));
const s3_1 = __importDefault(require("./libs/s3"));
function initStorage(config) {
    switch (config.type) {
        case 's3':
            return new s3_1.default(config);
        case 'minio':
            return new minio_1.default(config);
        default:
            throw new Error('wrong type ' + config.type);
    }
}
exports.initStorage = initStorage;
//# sourceMappingURL=index.js.map