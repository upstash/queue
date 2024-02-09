import { v4 as generateRandomUUID } from "uuid";

export const MQ_PREFIX = "UpstashMQ";

export const formatMessageQueueKey = (queueName: string) => {
  return `${MQ_PREFIX}:${queueName}`;
};

export const delay = (duration: number): Promise<void> => {
  return new Promise((resolve) => {
    setTimeout(resolve, duration);
  });
};

export const generateRandomConsumerName = (): string => {
  return `consumer-${generateRandomUUID()}`;
};

export type ParsedStreamMessage<TStreamResult> = {
  streamId: string;
  body: TStreamResult;
} | null;

type StartId = string;
type GroupName = string;
type StreamId = string;
type MessageBody = [string, string];

type XreadGroupStreamArray = [[GroupName, Array<[StreamId, MessageBody]>]];

/**
 * Parses the result of a Redis XREADGROUP response, extracting relevant information.
 *
 * @param {unknown[]} streamArray - The array representing the Redis XREADGROUP response.
 * Example structure: [["UpstashMQ:1251e0e7",[["1703755686316-0", ["messageBody", '{"hello":"world"}']]]]]
 *
 * @returns {ParsedStreamMessage<TStreamResult> | null} - Parsed result containing the stream ID
 * and parsed message body, or null if parsing fails.
 */
export const parseXreadGroupResponse = <TStreamResult>(
  streamArray: unknown[]
): ParsedStreamMessage<TStreamResult> => {
  const typedStream = streamArray as XreadGroupStreamArray;

  const streamData = typedStream?.[0];
  const firstMessage = streamData?.[1]?.[0];
  const streamId = firstMessage?.[0];
  const messageBodyString = firstMessage?.[1]?.[1];

  if (!streamId || !messageBodyString) {
    return null;
  }

  return {
    streamId: streamId,
    body: messageBodyString as any,
  };
};

type XclaimAutoRedisStreamArray = [StartId, Array<[StreamId, MessageBody]>];

/**
 * Parses the result of a Redis XCLAIM response with automatic stream creation,
 * extracting relevant information.
 *
 * @param {unknown[]} streamArray - The array representing the Redis XCLAIM response.
 * Example structure: ["0-0",[["1703754659687-0", ["messageBody", '{"hello":"world"}']]],[]]
 *
 * @returns {ParsedStreamMessage<TStreamResult> | null} - Parsed result containing the stream ID
 * and parsed message body, or null if parsing fails.
 */
export const parseXclaimAutoResponse = <TStreamResult>(
  streamArray: unknown[]
): ParsedStreamMessage<TStreamResult> => {
  const typedStream = streamArray as XclaimAutoRedisStreamArray;
  const firstMessage = typedStream?.[1]?.[0];
  const streamId = firstMessage?.[0];
  const messageBodyString = firstMessage?.[1]?.[1];

  if (!streamId || !messageBodyString) {
    return null;
  }

  return {
    streamId,
    body: messageBodyString as any,
  };
};

/**
 * Asserts that the provided data is non-null and non-undefined.
 * If the assertion fails, an error with the specified message is thrown.
 *
 * @param {T} data - The data to assert as non-nullable.
 * @param {string} message - The error message to throw if the assertion fails.
 * @throws {Error} Throws an error if the assertion fails.
 * @returns {asserts data is NonNullable<T>} - Type assertion indicating that the data is non-nullable.
 */
export function invariant<T>(data: T, message: string): asserts data is NonNullable<T> {
  if (!data) {
    throw new Error(message);
  }
}
