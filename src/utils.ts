export const MQ_PREFIX = "UpstashMQ";

export const formatMessageQueueKey = (queueName: string) => {
  return `${MQ_PREFIX}:${queueName}`;
};

export const delay = (duration: number): Promise<void> => {
  return new Promise((resolve) => {
    setTimeout(resolve, duration);
  });
};

/**
 * Generic retry function with exponential backoff.
 *
 * @param {Function} fn - The function to retry. Must return a Promise.
 * @param {number} maxAttempts - Maximum number of attempts.
 * @param {number} delay - Initial delay in milliseconds.
 * @returns {Promise<any>} - The result of the function `fn`.
 */
export const retryWithBackoff = async <T>(
  fn: () => Promise<T>,
  maxAttempts: number = 3,
  delay: number = 1000
): Promise<T> => {
  let attempts = 0;

  async function attempt() {
    try {
      return await fn();
    } catch (error) {
      attempts++;
      if (attempts >= maxAttempts) {
        throw error;
      }
      console.log(
        `Retrying in ${delay}ms... Attempt ${attempts}/${maxAttempts}`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay *= 2; // Exponential increase of delay
      return attempt();
    }
  }

  return attempt();
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

  let messageBody;
  try {
    messageBody = JSON.parse(messageBodyString);
  } catch (e) {
    console.error("Failed to parse message body:", e);
    return null;
  }

  return {
    streamId: streamId,
    body: messageBody,
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

  let messageBody;
  try {
    messageBody = JSON.parse(messageBodyString);
  } catch (e) {
    console.error("Failed to parse message body:", e);
    return null;
  }

  return {
    streamId,
    body: messageBody,
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
export function invariant<T>(
  data: T,
  message: string
): asserts data is NonNullable<T> {
  if (!data) {
    throw new Error(message);
  }
}
