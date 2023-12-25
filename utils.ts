const MQ_PREFIX = "UpstashMQ";

export const formatMessageQueueKey = (queueName: string) => {
  return `${MQ_PREFIX}:${queueName}`;
};

export const delay = (duration: number): Promise<void> => {
  return new Promise((resolve) => {
    setTimeout(resolve, duration);
  });
};
