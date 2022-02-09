import { ClientOptions } from 'minio'
import MinioStorage from './libs/minio'
import S3Storage from './libs/s3'

export function initStorage(config: { type: 'minio' | 's3' } & ClientOptions) {
  switch (config.type) {
    case 's3':
      return new S3Storage(config)
    case 'minio':
      return new MinioStorage(config)
    default:
      throw new Error('wrong type ' + config.type)
  }
}
