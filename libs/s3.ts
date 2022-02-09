import S3 from 'aws-sdk/clients/s3'
import fs from 'fs'
import fsPromise from 'fs/promises'
import stream from 'stream'

export default class S3Storage {
  s3: S3
  constructor(config: S3.ClientConfiguration) {
    this.s3 = new S3(config)
  }

  async uploadBuffer(options: { data: Buffer; destFile: string }) {
    if (!options || !options.data || !options.destFile) throw new Error('wrong options provided to upload')
    try {
      const bk = options.destFile.split('/')[1]
      const fileObject = options.destFile.split('/' + bk + '/')[1]
      console.log('uploading buffer to' + options.destFile)

      await this.s3.upload({ Bucket: bk, Key: fileObject, Body: options.data }).promise()
    } catch (err) {
      console.error('(uploadBuffer)', err)
      throw err
    }
  }

  async uploadFile(options: { sourceFile: string; destFile: string; removeSource?: boolean }) {
    if (!options || !options.sourceFile || !options.destFile) throw new Error('wrong options provided to upload')

    try {
      const stat = await fsPromise.stat(options.sourceFile)
      const mb = Math.round(stat.size / 1000000)

      if (mb < 2000) {
        const data = await fsPromise.readFile(options.sourceFile)
        await this.uploadBuffer({ data, destFile: options.destFile })
      } else {
        console.log(`uploading large file (${mb}MB) to ${options.destFile}`)

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
          const bk = options.destFile.split('/')[1]
          const fileObject = options.destFile.split('/' + bk + '/')[1]

          const pass = new stream.PassThrough()
          console.info('uploading part of ' + fileObject)
          return {
            writeStream: pass,
            promise: this.s3.upload({ Bucket: bk, Key: fileObject, Body: pass }).promise(),
          }
        }
        const { writeStream, promise } = uploadStream()
        const readStream = fs.createReadStream(options.sourceFile)
        readStream.pipe(writeStream)
        await promise
      }
      if (options.removeSource) {
        try {
          await fsPromise.rm(options.sourceFile)
        } catch (err) {
          console.error(`uploadToS3: Error while removing source ${options.sourceFile} (this could be just a warning)`)
        }
      }
      console.log(`uploading large file completed to ${options.destFile}`)

      return { ok: true }
    } catch (err) {
      console.error(`uploadToS3: Error while trying to upload file ${options.sourceFile} to ${options.destFile}`, err)
      throw err
    }
  }

  async getBuffer(sourceFile: string) {
    if (!sourceFile) throw new Error('No sourceFile Provided')

    const destFolder = await fsPromise.mkdtemp('/tmp/miniodwnlbuf')

    try {
      const bk = sourceFile.split('/')[1]
      const fileObject = sourceFile.split('/' + bk + '/')[1]
      const buf = await this.s3.getObject({ Bucket: bk, Key: fileObject }).promise()
      return buf.Body as Buffer
    } catch (err) {
      console.error(`getBuffer: Error while trying to download file from ${sourceFile}`)
      throw err
    } finally {
      fsPromise.rm(destFolder, { recursive: true }).catch(console.error)
    }
  }

  async removeFile(file: string) {
    if (!file) throw new Error('wrong options provided for remove action (Minio)')
    try {
      const bk = file.split('/')[1]
      const fileObject = file.split('/' + bk + '/')[1]
      await this.s3.deleteObject({ Bucket: bk, Key: fileObject }).promise()
      return { ok: true }
    } catch (err) {
      throw err
    }
  }

  async readFile(
    sourceFile: string,
    encoding: BufferEncoding = 'utf8',
    callback?: (error: Error | null, content?: any) => any
  ) {
    try {
      const buf = await this.getBuffer(sourceFile)
      const content = buf.toString(encoding)
      if (callback) callback(null, content)
      else return content
    } catch (err) {
      if (callback) callback(err)
      else throw err
    }
  }

  async readJson(
    sourceFile: string,
    encoding: BufferEncoding = 'utf8',
    callback?: (error: Error | null, content?: any) => any
  ) {
    if (callback) {
      await this.readFile(sourceFile, encoding, (error, content) => {
        if (error) callback(error)
        else callback(null, JSON.parse(content))
      })
    } else return JSON.parse(await this.readFile(sourceFile))
  }

  async downloadFile(options: { sourceFile: string; destFile: string }) {
    if (!options.sourceFile) throw new Error('No sourceFile Provided')

    try {
      const data = await this.getBuffer(options.sourceFile)
      await fsPromise.writeFile(options.destFile, data as any)
    } catch (err) {
      console.error(
        `downloadFile: Error while trying to download file from ${options.sourceFile} to ${options.destFile}`
      )
      throw err
    }
    return { ok: true }
  }

  getPresigned(options: { path: string; ssl?: boolean }) {
    if (!options.path) throw new Error('No s3 path Provided')

    const bk = options.path.split('/')[1]
    const fileObject = options.path.split('/' + bk + '/')[1]
    try {
      const presignedUrl = this.s3.getSignedUrl('getObject', { Bucket: bk, Key: fileObject, Expires: 6 * 60 * 60 })

      return { presignedUrl: presignedUrl }
    } catch (err) {
      throw err
    }
  }
}
