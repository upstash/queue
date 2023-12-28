export const DEFAULT_CONSUMER_GROUP_NAME = "Messages";
export const DEFAULT_CONSUMER_PREFIX = "Consumer";
export const DEFAULT_QUEUE_NAME = "Queue";
export const DEFAULT_CONCURRENCY_LIMIT = 0;
export const DEFAULT_AUTO_VERIFY = true;
export const DEFAULT_VISIBILITY_TIMEOUT_IN_MS = 1000 * 30;

export const MAX_CONCURRENCY_LIMIT = 5;

export const ERROR_MAP = {
  CONCURRENCY_LIMIT_EXCEEDED: `Cannot receive more than ${MAX_CONCURRENCY_LIMIT}`,
  CONCURRENCY_DEFAULT_LIMIT_EXCEEDED: `Cannot receive more than ${
    DEFAULT_CONCURRENCY_LIMIT + 1
  }, due to default limit not being set`,
} as const;
