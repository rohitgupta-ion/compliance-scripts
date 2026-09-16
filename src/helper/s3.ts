import { ListObjectsV2Command } from '@aws-sdk/client-s3'
import { s3 } from '../infra/aws'

export const listGzipKeys = async (bucket: string, prefix: string): Promise<string[]> => {
  const keys: string[] = []
  let continuationToken: string | undefined

  do {
    const response = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      }),
    )

    for (const object of response.Contents ?? []) {
      if (object.Key && object.Key.endsWith('.gz')) {
        // eslint-disable-next-line functional/immutable-data
        keys.push(object.Key)
      }
    }

    continuationToken = response.NextContinuationToken
  } while (continuationToken)

  return keys
}
