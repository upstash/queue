const MQ_PREFIX = "UpstashMQ";

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
export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxAttempts: number = 3,
  delay: number = 1000
): Promise<T> {
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
}
