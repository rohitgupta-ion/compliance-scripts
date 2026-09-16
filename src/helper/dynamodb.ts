export type DynamoDbAttributeValue = Record<string, unknown>

export const unmarshallAttribute = (attribute: DynamoDbAttributeValue): unknown => {
  if ('S' in attribute) return attribute.S
  if ('N' in attribute) return Number(attribute.N)
  if ('BOOL' in attribute) return attribute.BOOL
  if ('NULL' in attribute) return null
  if ('SS' in attribute) return attribute.SS
  if ('NS' in attribute) return (attribute.NS as string[]).map(Number)
  if ('L' in attribute) return (attribute.L as DynamoDbAttributeValue[]).map(unmarshallAttribute)
  if ('M' in attribute) return unmarshallItem(attribute.M as Record<string, DynamoDbAttributeValue>)
  return undefined
}

export const unmarshallItem = (map: Record<string, DynamoDbAttributeValue>): Record<string, unknown> => {
  const result: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(map)) {
    result[key] = unmarshallAttribute(value)
  }
  return result
}
