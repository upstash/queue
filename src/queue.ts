import Redis from "ioredis";
import crypto from "node:crypto";
import {
  DEFAULT_AUTO_VERIFY,
  DEFAULT_CONCURRENCY_LIMIT,
  DEFAULT_CONSUMER_GROUP_NAME,
  DEFAULT_CONSUMER_PREFIX,
  DEFAULT_QUEUE_NAME,
  DEFAULT_VISIBILITY_TIMEOUT_IN_MS,
  ERROR_MAP,
  MAX_CONCURRENCY_LIMIT,
} from "./constants";
import {
  ParsedStreamMessage,
  formatMessageQueueKey,
  invariant,
  parseXclaimAutoResponse,
  parseXreadGroupResponse,
} from "./utils";

export type QueueConfig = {
  redis: Redis;
  queueName?: string;
  concurrencyLimit?: 0 | 1 | 2 | 3 | 4 | 5;
  autoVerify?: boolean;
  consumerGroupName?: string;
  consumerNamePrefix?: string;
  visibilityTimeout?: number;
};

export class Queue {
  config: QueueConfig;
  concurrencyCounter = DEFAULT_CONCURRENCY_LIMIT;

  private messageTimeouts = new Set<NodeJS.Timer>();

  constructor(config: QueueConfig) {
    this.config = {
      redis: config.redis,

      concurrencyLimit: config.concurrencyLimit ?? DEFAULT_CONCURRENCY_LIMIT,
      autoVerify: config.autoVerify ?? DEFAULT_AUTO_VERIFY,
      consumerGroupName:
        config.consumerGroupName ?? DEFAULT_CONSUMER_GROUP_NAME,
      consumerNamePrefix: config.consumerNamePrefix ?? DEFAULT_CONSUMER_PREFIX,
      queueName: config.queueName
        ? this.appendPrefixTo(config.queueName)
        : this.appendPrefixTo(DEFAULT_QUEUE_NAME),
      visibilityTimeout:
        config.visibilityTimeout ?? DEFAULT_VISIBILITY_TIMEOUT_IN_MS,
    };
    this.initializeConsumerGroup();
    this.setupShutdownHandler();
  }

  private appendPrefixTo(key: string) {
    return formatMessageQueueKey(key);
  }

  private async initializeConsumerGroup() {
    invariant(
      this.config.consumerGroupName,
      "Consumer group name cannot be empty when initializing consumer group"
    );
    invariant(
      this.config.queueName,
      "Queue name cannot be empty when initializing consumer group"
    );

    await this.config.redis.xgroup(
      "CREATE",
      this.config.queueName,
      this.config.consumerGroupName,
      "$",
      "MKSTREAM"
    );
  }

  async sendMessage<T extends {}>(payload: T, delayInSeconds: number = 0) {
    const { redis } = this.config;
    try {
      const flattenedPayload = Object.entries({
        messageBody: JSON.stringify(payload),
      }).flat() as string[];

      const streamKey = this.config.queueName;
      invariant(streamKey, "Queue name cannot be empty when sending a message");

      const _sendMessage = () =>
        redis.xadd(streamKey, "*", ...flattenedPayload);

      if (delayInSeconds > 0) {
        let streamIdResult: string | null = null;

        const timeoutId = setTimeout(() => {
          _sendMessage().then((res) => {
            streamIdResult = res;
            this.messageTimeouts.delete(timeoutId);
          });
        }, delayInSeconds * 1000);
        this.messageTimeouts.add(timeoutId);
        return streamIdResult;
      } else {
        return await _sendMessage();
      }
    } catch (error) {
      console.error("Error in sendMessage:", error);
      return null;
    }
  }

  async receiveMessage<TStreamResult>(blockTimeMs = 0) {
    this.checkIfReceiveMessageAllowed();

    const xclaimParsedMessage =
      await this.claimStuckPendingMessageAndVerify<TStreamResult>();

    if (xclaimParsedMessage) {
      return xclaimParsedMessage;
    }

    // Claiming failed, fallback to default read message
    return await this.readAndVerifyPendingMessage<TStreamResult>(blockTimeMs);
  }

  private checkIfReceiveMessageAllowed() {
    const { concurrencyLimit } = this.config;

    const concurrencyNotSetAndAboveDefaultLimit =
      concurrencyLimit === DEFAULT_CONCURRENCY_LIMIT &&
      this.concurrencyCounter >= DEFAULT_CONCURRENCY_LIMIT + 1;
    const concurrencyAboveTheMaxLimit =
      this.concurrencyCounter > MAX_CONCURRENCY_LIMIT;

    if (concurrencyNotSetAndAboveDefaultLimit) {
      throw new Error(ERROR_MAP.CONCURRENCY_DEFAULT_LIMIT_EXCEEDED);
    }

    this.incrementConcurrencyCount();

    if (concurrencyAboveTheMaxLimit) {
      throw new Error(ERROR_MAP.CONCURRENCY_LIMIT_EXCEEDED);
    }
  }

  private async claimStuckPendingMessageAndVerify<
    TStreamResult
  >(): Promise<ParsedStreamMessage<TStreamResult> | null> {
    const { autoVerify } = this.config;
    const consumerName = this.generateRandomConsumerName();

    const xclaimParsedMessage = await this.claimAndParseMessage<TStreamResult>(
      consumerName
    );

    if (xclaimParsedMessage && autoVerify) {
      await this.verifyMessage(xclaimParsedMessage.streamId);
    }

    return xclaimParsedMessage;
  }

  private async claimAndParseMessage<TStreamResult>(
    consumerName: string
  ): Promise<ParsedStreamMessage<TStreamResult> | null> {
    const xclaimRes = await this.autoClaim(consumerName);
    return parseXclaimAutoResponse<TStreamResult>(xclaimRes);
  }

  private async readAndVerifyPendingMessage<TStreamResult>(
    blockTimeMs: number
  ): Promise<ParsedStreamMessage<TStreamResult> | null> {
    const { autoVerify } = this.config;

    const parsedXreadMessage = await this.readAndParseMessage<TStreamResult>(
      blockTimeMs
    );

    if (parsedXreadMessage && autoVerify) {
      await this.verifyMessage(parsedXreadMessage.streamId);
    }

    return parsedXreadMessage;
  }

  async readAndParseMessage<StreamResult>(
    blockTimeMs: number
  ): Promise<ParsedStreamMessage<StreamResult> | null> {
    const consumerName = this.generateRandomConsumerName();

    const xreadRes =
      blockTimeMs > 0
        ? await this.receiveBlockingMessage(blockTimeMs)
        : await this.receiveNonBlockingMessage(consumerName);

    return parseXreadGroupResponse<StreamResult>(xreadRes);
  }

  private async autoClaim(consumerName: string) {
    const { redis, consumerGroupName, queueName, visibilityTimeout } =
      this.config;
    invariant(
      consumerGroupName,
      "Consumer group name cannot be empty when receiving a message"
    );
    invariant(queueName, "Queue name cannot be empty when receving a message");
    invariant(
      visibilityTimeout,
      "Visibility timeout name cannot be empty when receving a message"
    );

    return await redis.xautoclaim(
      queueName,
      consumerGroupName,
      consumerName,
      visibilityTimeout,
      0 - 0,
      "COUNT",
      1
    );
  }

  private async receiveBlockingMessage(blockTimeMs: number) {
    const { redis, consumerGroupName, queueName } = this.config;
    invariant(
      consumerGroupName,
      "Consumer group name cannot be empty when receiving a message"
    );
    invariant(queueName, "Queue name cannot be empty when receving a message");
    const consumerName = this.generateRandomConsumerName();

    return redis.xreadgroup(
      "GROUP",
      consumerGroupName,
      consumerName,
      "COUNT",
      1,
      "BLOCK",
      blockTimeMs,
      "STREAMS",
      queueName,
      ">"
    );
  }

  private async receiveNonBlockingMessage(consumerName: string) {
    const { redis, consumerGroupName, queueName } = this.config;
    invariant(
      consumerGroupName,
      "Consumer group name cannot be empty when receiving a message"
    );
    invariant(queueName, "Queue name cannot be empty when receving a message");

    return redis.xreadgroup(
      "GROUP",
      consumerGroupName,
      consumerName,
      "COUNT",
      1,
      "STREAMS",
      queueName,
      ">"
    );
  }

  async verifyMessage(streamId: string): Promise<"VERIFIED" | "NOT VERIFIED"> {
    const { redis } = this.config;

    try {
      invariant(
        this.config.consumerGroupName,
        "Consumer group name cannot be empty when verifying a message"
      );

      invariant(
        this.config.queueName,
        "Queue name cannot be empty when verifying a message"
      );

      const xackRes = await redis.xack(
        this.config.queueName,
        this.config.consumerGroupName,
        streamId
      );
      if (typeof xackRes === "number" && xackRes > 0) {
        this.decrementConcurrencyCount();
        return "VERIFIED";
      }
      return "NOT VERIFIED";
    } catch (finalError) {
      console.error(
        `Final attempt to acknowledge message failed: ${
          (finalError as Error).message
        }`
      );
      return "NOT VERIFIED";
    }
  }

  private setupShutdownHandler() {
    process.on("SIGINT", this.shutdown.bind(this));
    process.on("SIGTERM", this.shutdown.bind(this));
  }

  private async shutdown() {
    console.log("Shutting down gracefully...");
    this.messageTimeouts.forEach((timeoutId) => clearTimeout(timeoutId));
    // Add any other cleanup logic here
    process.exit(0);
  }

  private generateRandomConsumerName = () => {
    if (this.concurrencyCounter > MAX_CONCURRENCY_LIMIT)
      throw new Error(ERROR_MAP.CONCURRENCY_LIMIT_EXCEEDED);
    const randomUUID = crypto.randomUUID();
    return this.appendPrefixTo(randomUUID);
  };

  private incrementConcurrencyCount() {
    this.concurrencyCounter++;
  }

  private decrementConcurrencyCount() {
    this.concurrencyCounter--;
  }
}
