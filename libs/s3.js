"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const s3_1 = __importDefault(require("aws-sdk/clients/s3"));
const fs_1 = __importDefault(require("fs"));
const promises_1 = __importDefault(require("fs/promises"));
const stream_1 = __importDefault(require("stream"));
class S3Storage {
    constructor(config) {
        this.s3 = new s3_1.default(config);
    }
    async uploadBuffer(options) {
        if (!options || !options.data || !options.destFile)
            throw new Error('wrong options provided to upload');
        try {
            const bk = options.destFile.split('/')[1];
            const fileObject = options.destFile.split('/' + bk + '/')[1];
            console.log('uploading buffer to' + options.destFile);
            await this.s3.upload({ Bucket: bk, Key: fileObject, Body: options.data }).promise();
        }
        catch (err) {
            console.error('(uploadBuffer)', err);
            throw err;
        }
    }
    async uploadFile(options) {
        if (!options || !options.sourceFile || !options.destFile)
            throw new Error('wrong options provided to upload');
        try {
            const stat = await promises_1.default.stat(options.sourceFile);
            const mb = Math.round(stat.size / 1000000);
            if (mb < 2000) {
                const data = await promises_1.default.readFile(options.sourceFile);
                await this.uploadBuffer({ data, destFile: options.destFile });
            }
            else {
                console.log(`uploading large file (${mb}MB) to ${options.destFile}`);
                // function uploadLargeFile(S3) {
                //   return new Promise((resolve, reject) => {
                //     const readStream = fs.createReadStream(options.sourceFile)
                //     function s3StreamUploader() {
                //       const pass = new stream.PassThrough()
                //       const bk = options.destFile.split('/')[1]
                //       const fileObject = options.destFile.split('/' + bk + '/')[1]
                //       const params = {
                //         Bucket: bk,
                //         Key: fileObject,
                //         Body: pass
                //       }
                //       S3.upload(params, function(error, data) {
                //         if (error) return reject(error)
                //         console.info('uploading part for ' + fileObject)
                //       })
                //       S3.on('uploaded', function(details) {
                //         resolve(true)
                //       })
                //       return pass
                //     }
                //     readStream.pipe(s3StreamUploader())
                //   })
                // }
                // try {
                //   await uploadLargeFile(this.s3)
                // } catch (err) {
                //   throw err
                // }
                const uploadStream = () => {
                    const bk = options.destFile.split('/')[1];
                    const fileObject = options.destFile.split('/' + bk + '/')[1];
                    const pass = new stream_1.default.PassThrough();
                    console.info('uploading part of ' + fileObject);
                    return {
                        writeStream: pass,
                        promise: this.s3.upload({ Bucket: bk, Key: fileObject, Body: pass }).promise(),
                    };
                };
                const { writeStream, promise } = uploadStream();
                const readStream = fs_1.default.createReadStream(options.sourceFile);
                readStream.pipe(writeStream);
                await promise;
            }
            if (options.removeSource) {
                try {
                    await promises_1.default.rm(options.sourceFile);
                }
                catch (err) {
                    console.error(`uploadToS3: Error while removing source ${options.sourceFile} (this could be just a warning)`);
                }
            }
            console.log(`uploading large file completed to ${options.destFile}`);
            return { ok: true };
        }
        catch (err) {
            console.error(`uploadToS3: Error while trying to upload file ${options.sourceFile} to ${options.destFile}`, err);
            throw err;
        }
    }
    async getBuffer(sourceFile) {
        if (!sourceFile)
            throw new Error('No sourceFile Provided');
        const destFolder = await promises_1.default.mkdtemp('/tmp/miniodwnlbuf');
        try {
            const bk = sourceFile.split('/')[1];
            const fileObject = sourceFile.split('/' + bk + '/')[1];
            const buf = await this.s3.getObject({ Bucket: bk, Key: fileObject }).promise();
            return buf.Body;
        }
        catch (err) {
            console.error(`getBuffer: Error while trying to download file from ${sourceFile}`);
            throw err;
        }
        finally {
            promises_1.default.rm(destFolder, { recursive: true }).catch(console.error);
        }
    }
    async removeFile(file) {
        if (!file)
            throw new Error('wrong options provided for remove action (Minio)');
        try {
            const bk = file.split('/')[1];
            const fileObject = file.split('/' + bk + '/')[1];
            await this.s3.deleteObject({ Bucket: bk, Key: fileObject }).promise();
            return { ok: true };
        }
        catch (err) {
            throw err;
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
        try {
            const data = await this.getBuffer(options.sourceFile);
            await promises_1.default.writeFile(options.destFile, data);
        }
        catch (err) {
            console.error(`downloadFile: Error while trying to download file from ${options.sourceFile} to ${options.destFile}`);
            throw err;
        }
        return { ok: true };
    }
    getPresigned(options) {
        if (!options.path)
            throw new Error('No s3 path Provided');
        const bk = options.path.split('/')[1];
        const fileObject = options.path.split('/' + bk + '/')[1];
        try {
            const presignedUrl = this.s3.getSignedUrl('getObject', { Bucket: bk, Key: fileObject, Expires: 6 * 60 * 60 });
            return { presignedUrl: presignedUrl };
        }
        catch (err) {
            throw err;
        }
    }
}
exports.default = S3Storage;
//# sourceMappingURL=s3.js.map