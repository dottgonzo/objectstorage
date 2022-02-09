"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const promises_1 = __importDefault(require("fs/promises"));
const mime_types_1 = __importDefault(require("mime-types"));
const minio_1 = require("minio");
const moment_1 = __importDefault(require("moment"));
const path_1 = __importDefault(require("path"));
class MinioStorage extends minio_1.Client {
    constructor(config) {
        super(config);
    }
    ls(minioDirPath, options) {
        if (!minioDirPath)
            throw new Error('wrong options provided for ls action (Minio)');
        if (!options)
            options = {};
        if (!options.recursive)
            options.recursive = false;
        return new Promise((resolve, reject) => {
            const bk = minioDirPath.split('/')[1];
            let prefix = minioDirPath.split('/' + bk + '/')[1];
            if (prefix && prefix[prefix.length - 1] !== '/')
                prefix = prefix + '/';
            const stream = this.listObjectsV2(bk, prefix, options.recursive);
            const fileList = [];
            stream.on('data', (obj) => {
                if (obj && obj.lastModified && obj.name) {
                    const dateOfFile = (0, moment_1.default)(obj.lastModified);
                    fileList.push({
                        filePath: '/' + bk + '/' + obj.name,
                        fileName: path_1.default.basename(obj.name),
                        lastModified: dateOfFile.format(),
                        unixTime: dateOfFile.valueOf(),
                    });
                }
            });
            stream.on('end', () => {
                if (options.sorted)
                    fileList.sort((f) => f.unixTime);
                resolve(fileList);
            });
            stream.on('error', (err) => {
                reject(err);
            });
        });
    }
    async copyFolderOnMinio(opts) {
        try {
            if (!opts || !opts.sourceFolder || !opts.destFolder)
                throw new Error('wrong moveOnMinio opts');
            const allMinioFiles = await this.ls(opts.sourceFolder);
            const allMinioFilesPath = allMinioFiles.map((m) => m.filePath);
            for (const minioFile of allMinioFilesPath) {
                await this.copyFileOnMinio({
                    sourceFile: minioFile,
                    destFile: opts.destFolder + '/' + path_1.default.basename(minioFile),
                });
            }
        }
        catch (err) {
            console.error(err);
            throw err;
        }
    }
    async moveFolderOnMinio(opts) {
        await this.copyFolderOnMinio(opts);
        await this.removeFolderFromMinio(opts.sourceFolder);
    }
    async moveFileOnMinio(opts) {
        await this.copyFileOnMinio(opts);
        await this.removeFile(opts.sourceFile);
    }
    async copyFileOnMinio(opts) {
        try {
            if (!opts || !opts.sourceFile || !opts.destFile)
                throw new Error('wrong moveOnMinio opts');
            const bkSource = opts.sourceFile.split('/')[1];
            const bkDest = opts.destFile.split('/')[1];
            if (bkSource === bkDest) {
                const fileObjectDest = opts.destFile.split('/' + bkSource + '/')[1];
                const fileObjectSource = opts.sourceFile.split('/' + bkSource + '/')[1];
                var conds = new minio_1.CopyConditions();
                // const ob=await this.statObject(bkSource, fileObjectSource)
                // conds.setMatchETag(ob.etag)
                const copyo = await this.copyObject(bkSource, fileObjectDest, opts.sourceFile, conds);
                // console.log('Successfully copied the object:')
            }
            else {
                const buff = await this.getBuffer(opts.sourceFile);
                await this.uploadBuffer({ destFile: opts.destFile, data: buff });
            }
        }
        catch (err) {
            console.error(err);
            throw err;
        }
    }
    async statFileOnMinio(minioFilePath) {
        if (!minioFilePath)
            throw new Error('wrong options provided for remove action (Minio)');
        try {
            const bk = minioFilePath.split('/')[1];
            const fileObject = minioFilePath.split('/' + bk + '/')[1];
            return await this.statObject(bk, fileObject);
        }
        catch (err) {
            throw err;
        }
    }
    async checkIfExistsOnMinio(minioFilePath) {
        if (!minioFilePath)
            throw new Error('wrong options provided for remove action (Minio)');
        try {
            const bk = minioFilePath.split('/')[1];
            const fileObject = minioFilePath.split('/' + bk + '/')[1];
            try {
                const statFile = await this.statObject(bk, fileObject);
                // await this.removeObject(bk, fileObject)
                return true;
            }
            catch (err) {
                return false;
            }
        }
        catch (err) {
            throw err;
        }
    }
    async removeFolderFromMinio(minioFolder) {
        // TODO: to be tested
        try {
            if (!minioFolder)
                throw new Error('wrong options provided for remove action (Minio)');
            const files = await this.ls(minioFolder);
            for (const file of files) {
                await this.removeFile(file.filePath);
            }
        }
        catch (err) {
            console.error(err);
            throw err;
        }
    }
    async removeFile(file) {
        if (!file)
            throw new Error('wrong options provided for remove action (Minio)');
        try {
            const bk = file.split('/')[1];
            const fileObject = file.split('/' + bk + '/')[1];
            await this.removeObject(bk, fileObject);
            return { ok: true };
        }
        catch (err) {
            throw err;
        }
    }
    async uploadBuffer(options) {
        if (!options || !options.data || !options.destFile)
            throw new Error('wrong options provided to upload');
        try {
            const tmpFile = '/tmp/' + Date.now() + '_' + path_1.default.basename(options.destFile);
            await promises_1.default.writeFile(tmpFile, options.data);
            await this.uploadFile({
                sourceFile: tmpFile,
                destFile: options.destFile,
                removeSource: true,
            });
        }
        catch (err) {
            console.error('(uploadBuffer)');
            throw err;
        }
    }
    async uploadFile(options) {
        if (!options || !options.sourceFile || !options.destFile)
            throw new Error('wrong options provided to upload');
        if (!options.meta || !options.meta['Content-Type']) {
            options.meta = {
                'Content-Type': mime_types_1.default.lookup(path_1.default.basename(options.destFile)) || 'application/octet-stream',
            };
        }
        const bk = options.destFile.split('/')[1];
        const fileObject = options.destFile.split('/' + bk + '/')[1];
        try {
            await this.fPutObject(bk, fileObject, options.sourceFile, options.meta);
            if (options.removeSource) {
                try {
                    await promises_1.default.rm(options.sourceFile, { recursive: true });
                }
                catch (err) {
                    console.error(`uploadToMinio: Error while removing source ${options.sourceFile} (this could be just a warning)`);
                }
            }
            return { ok: true };
        }
        catch (err) {
            console.error(`uploadToMinio: Error while trying to upload file ${options.sourceFile} to ${options.destFile}`);
            throw err;
        }
    }
    async getBuffer(sourceFile) {
        if (!sourceFile)
            throw new Error('No sourceFile Provided');
        const destFolder = await promises_1.default.mkdtemp('/tmp/miniodwnlbuf');
        try {
            const destFile = path_1.default.join(destFolder, path_1.default.basename(sourceFile));
            await this.downloadFile({ sourceFile, destFile });
            const buf = await promises_1.default.readFile(destFile);
            return buf;
        }
        catch (err) {
            console.error(`getBufferFromMinio: Error while trying to download file from ${sourceFile}`);
            throw err;
        }
        finally {
            promises_1.default.rm(destFolder, { recursive: true }).catch(console.error);
        }
    }
    async readFile(sourceFile, encoding = 'utf8', callback) {
        try {
            const buf = await this.getBuffer(sourceFile);
            const content = buf.toString(encoding);
            if (callback)
                callback(null, content);
            else
                return content;
        }
        catch (err) {
            if (callback)
                callback(err);
            else
                throw err;
        }
    }
    async readJson(sourceFile, encoding = 'utf8', callback) {
        if (callback) {
            await this.readFile(sourceFile, encoding, (error, content) => {
                if (error)
                    callback(error);
                else
                    callback(null, JSON.parse(content));
            });
        }
        else
            return JSON.parse(await this.readFile(sourceFile));
    }
    async downloadFile(options) {
        if (!options.sourceFile)
            throw new Error('No sourceFile Provided');
        const bk = options.sourceFile.split('/')[1];
        const fileObject = options.sourceFile.split('/' + bk + '/')[1];
        try {
            await this.fGetObject(bk, fileObject, options.destFile);
        }
        catch (err) {
            console.error(`downloadFromMinio: Error while trying to download file from ${options.sourceFile} to ${options.destFile}`);
            throw err;
        }
        return { ok: true };
    }
    async getPresigned(options) {
        if (!options.path)
            throw new Error('No minioFilePath Provided');
        let protocol = 'http';
        if (options.ssl)
            protocol = 'https';
        const bk = options.path.split('/')[1];
        const fileObject = options.path.split('/' + bk + '/')[1];
        try {
            const presignedUrl = await this.presignedGetObject(bk, fileObject, 6 * 60 * 60);
            return { presignedUrl: presignedUrl };
        }
        catch (err) {
            throw err;
        }
    }
}
exports.default = MinioStorage;
//# sourceMappingURL=minio.js.map