import { GetObjectCommand } from '@aws-sdk/client-s3'
import * as fs from 'fs'
import * as path from 'path'
import * as readline from 'readline'
import { Readable } from 'stream'
import { createGunzip } from 'zlib'
import { s3 } from './infra/aws'
import { DynamoDbAttributeValue, unmarshallItem } from './infra/dynamodb'
import { listGzipKeys } from './read-s3-gzip-objects'

const bucketName = 'live-compliance-monitor-audit-trail'
const prefix = 'errors'

type CustomerEvent = {
  timestamp: string
  event?: string
  worklistId?: string
  monitorRecordId?: string
}

type CustomerEventsSummary = {
  totalEvents: number
  events: CustomerEvent[]
}

const getCustomerId = (item: Record<string, unknown>): string | undefined => {
  const properties = item.properties as Record<string, unknown> | undefined
  const customerId = item.customerId ?? properties?.customerId
  return typeof customerId === 'string' ? customerId : undefined
}

const getWorklistId = (item: Record<string, unknown>): string | undefined => {
  const properties = item.properties as Record<string, unknown> | undefined
  const worklistId = item.worklistId ?? properties?.worklistId
  return typeof worklistId === 'string' ? worklistId : undefined
}

const getMonitorRecordId = (item: Record<string, unknown>): string | undefined => {
  return typeof item.gsi1hash === 'string' ? item.gsi1hash : undefined
}

// Falls back to the DynamoDB stream's own creation time when the item has no timestamp field.
const getEventTimestamp = (item: Record<string, unknown>, approximateCreationDateTime: number | undefined): string => {
  const itemTimestamp = item.updatedDateTime ?? item.timestamp
  if (typeof itemTimestamp === 'string') {
    return itemTimestamp
  }
  return approximateCreationDateTime ? new Date(approximateCreationDateTime * 1000).toISOString() : new Date(0).toISOString()
}

const collectCustomerEventsFromObject = async (
  bucket: string,
  key: string,
  customerEvents: Map<string, CustomerEvent[]>,
): Promise<void> => {
  const { Body } = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }),
  )

  const decompressedStream = (Body as Readable).pipe(createGunzip())
  const rl = readline.createInterface({ input: decompressedStream, crlfDelay: Infinity })

  for await (const line of rl) {
    if (!line.trim()) {
      continue
    }

    const parsedLine = JSON.parse(line)
    const decodedData = JSON.parse(Buffer.from(parsedLine.rawData, 'base64').toString())

    const newImage = decodedData.dynamodb?.NewImage as Record<string, DynamoDbAttributeValue> | undefined
    if (!newImage) {
      continue
    }

    const item = unmarshallItem(newImage)
    const customerId = getCustomerId(item)
    if (!customerId) {
      continue
    }

    const event: CustomerEvent = {
      timestamp: getEventTimestamp(item, decodedData.dynamodb?.ApproximateCreationDateTime),
      event: decodedData.eventName,
      worklistId: getWorklistId(item),
      monitorRecordId: getMonitorRecordId(item),
    }

    const events = customerEvents.get(customerId) ?? []
    // eslint-disable-next-line functional/immutable-data
    events.push(event)
    customerEvents.set(customerId, events)
  }
}

const collectCustomerEventsFromS3 = async (bucket: string, prefix: string): Promise<Map<string, CustomerEvent[]>> => {
  const keys = await listGzipKeys(bucket, prefix)

  const customerEvents = new Map<string, CustomerEvent[]>()
  for (const key of keys) {
    await collectCustomerEventsFromObject(bucket, key, customerEvents)
  }

  return customerEvents
}

const buildCustomerEventsSummary = (
  customerEvents: Map<string, CustomerEvent[]>,
): Record<string, CustomerEventsSummary> => {
  const summary: Record<string, CustomerEventsSummary> = {}

  for (const [customerId, events] of customerEvents) {
    const sortedEvents = [...events].sort((a, b) => b.timestamp.localeCompare(a.timestamp))
    summary[customerId] = {
      totalEvents: sortedEvents.length,
      events: sortedEvents,
    }
  }

  return summary
}

const writeResults = (summary: Record<string, CustomerEventsSummary>): string => {
  const resultsDir = path.join(__dirname, '..', 'results')
  fs.mkdirSync(resultsDir, { recursive: true })

  const outputPath = path.join(resultsDir, `customer-event-counts-${Date.now()}.json`)
  fs.writeFileSync(outputPath, JSON.stringify(summary, null, 2))

  return outputPath
}

const main = async () => {
  const customerEvents = await collectCustomerEventsFromS3(bucketName, prefix)
  const summary = buildCustomerEventsSummary(customerEvents)
  const outputPath = writeResults(summary)

  console.log(`Tallied events for ${Object.keys(summary).length} customers from s3://${bucketName}/${prefix}`)
  console.log(`Results written to ${outputPath}`)
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error)
  })
}
