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

type RedisStreamMessage = [string, string];

type RedisStreamEntry = [string, RedisStreamMessage[]];

type RedisStreamArray = RedisStreamEntry[];

export const parseRedisStreamMessage = <StreamResult>(
  streamArray: unknown[]
): { streamId: string; body: StreamResult } | null => {
  const typedStream = streamArray as RedisStreamArray;
  // Safely access nested elements
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

export function invariant<T>(
  data: T,
  message: string
): asserts data is NonNullable<T> {
  if (!data) {
    throw new Error(message);
  }
}
