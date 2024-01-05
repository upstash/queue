import crypto from "node:crypto";
import {
  DEFAULT_AUTO_VERIFY,
  DEFAULT_CONCURRENCY_LIMIT,
  DEFAULT_CONSUMER_GROUP_NAME,
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

import { Redis } from "@upstash/redis";

export type QueueConfig = {
  redis: Redis;
  /**
   * Queue name for the redis stream
   * @default "UpstashMQ:Queue"
   */
  queueName?: string;
  /**
   * The maximum number of concurrent message processing allowed.
   * @default 1
   */
  concurrencyLimit?: 0 | 1 | 2 | 3 | 4 | 5;
  /**
   * Auto verifies received messages. If not set message will be picked up by some other consumer after visiblityTimeout.
   * @default true
   */
  autoVerify?: boolean;
  /**
   * This is the group that holds every other consumer when automatically created.
   * @default "Messages"
   */
  consumerGroupName?: string;
  /**
   * Recently sent messages won't be visible to other consumers until this period of time. If no one else acknowledges it it will be picked up by others.
   * @default "30 seconds"
   */
  visibilityTimeout?: number;
};

export class Queue {
  config: QueueConfig;
  concurrencyCounter = DEFAULT_CONCURRENCY_LIMIT;
  hasConsumerGroupInitialized = false;

  private messageTimeouts = new Set<NodeJS.Timer>();

  constructor(config: QueueConfig) {
    this.config = {
      redis: config.redis,

      concurrencyLimit: config.concurrencyLimit ?? DEFAULT_CONCURRENCY_LIMIT,
      autoVerify: config.autoVerify ?? DEFAULT_AUTO_VERIFY,
      consumerGroupName: config.consumerGroupName ?? DEFAULT_CONSUMER_GROUP_NAME,
      queueName: config.queueName
        ? this.appendPrefixTo(config.queueName)
        : this.appendPrefixTo(DEFAULT_QUEUE_NAME),
      visibilityTimeout: config.visibilityTimeout ?? DEFAULT_VISIBILITY_TIMEOUT_IN_MS,
    };
  }

  private appendPrefixTo(key: string) {
    return formatMessageQueueKey(key);
  }

  private async initializeConsumerGroup() {
    if (this.hasConsumerGroupInitialized) {
      return;
    }

    invariant(
      this.config.consumerGroupName,
      "Consumer group name cannot be empty when initializing consumer group"
    );
    invariant(this.config.queueName, "Queue name cannot be empty when initializing consumer group");

    try {
      await this.config.redis.xgroup(this.config.queueName, {
        type: "CREATE",
        group: this.config.consumerGroupName,
        id: "$",
        options: { MKSTREAM: true },
      });
      this.hasConsumerGroupInitialized = true;
    } catch {
      return null;
    }
  }

  async sendMessage<T extends {}>(payload: T, delayMs = 0) {
    const { redis } = this.config;
    await this.initializeConsumerGroup();
    try {
      const streamKey = this.config.queueName;
      invariant(streamKey, "Queue name cannot be empty when sending a message");

      const _sendMessage = () =>
        redis.xadd(streamKey, "*", {
          messageBody: payload,
        });

      if (delayMs > 0) {
        let streamIdResult: string | null = null;

        const timeoutId = setTimeout(() => {
          _sendMessage().then((res) => {
            streamIdResult = res;
            this.messageTimeouts.delete(timeoutId);
          });
        }, delayMs);
        this.messageTimeouts.add(timeoutId);
        return streamIdResult;
      }
      return await _sendMessage();
    } catch (error) {
      console.error("Error in sendMessage:", error);
      return null;
    }
  }

  async receiveMessage<TStreamResult>(blockTimeMs = 0) {
    this.checkIfReceiveMessageAllowed();
    await this.initializeConsumerGroup();

    const xclaimParsedMessage = await this.claimStuckPendingMessageAndVerify<TStreamResult>();
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

    const concurrencyAboveTheMaxLimit = this.concurrencyCounter > MAX_CONCURRENCY_LIMIT;

    if (concurrencyNotSetAndAboveDefaultLimit) {
      throw new Error(ERROR_MAP.CONCURRENCY_DEFAULT_LIMIT_EXCEEDED);
    }

    this.incrementConcurrencyCount();

    if (concurrencyAboveTheMaxLimit) {
      throw new Error(ERROR_MAP.CONCURRENCY_LIMIT_EXCEEDED);
    }
  }

  private async claimStuckPendingMessageAndVerify<TStreamResult>(): Promise<
    ParsedStreamMessage<TStreamResult>
  > {
    const { autoVerify } = this.config;
    const consumerName = this.generateRandomConsumerName();

    const xclaimParsedMessage = await this.claimAndParseMessage<TStreamResult>(consumerName);

    if (xclaimParsedMessage && autoVerify) {
      await this.verifyMessage(xclaimParsedMessage.streamId);
    }

    if (xclaimParsedMessage == null) {
      await this.removeEmptyConsumer(consumerName);
    }

    return xclaimParsedMessage;
  }

  private async removeEmptyConsumer(consumer: string) {
    const { redis, consumerGroupName, queueName } = this.config;
    invariant(consumerGroupName, "Consumer group name cannot be empty when removing a consumer");
    invariant(queueName, "Queue name cannot be empty when removing a consumer");

    await redis.xgroup(queueName, {
      type: "DELCONSUMER",
      consumer,
      group: consumerGroupName,
    });
  }

  private async claimAndParseMessage<TStreamResult>(
    consumerName: string
  ): Promise<ParsedStreamMessage<TStreamResult>> {
    const xclaimRes = await this.autoClaim(consumerName);
    return parseXclaimAutoResponse<TStreamResult>(xclaimRes);
  }

  private async autoClaim(consumerName: string) {
    const { redis, consumerGroupName, queueName, visibilityTimeout } = this.config;
    invariant(consumerGroupName, "Consumer group name cannot be empty when receiving a message");
    invariant(queueName, "Queue name cannot be empty when receving a message");
    invariant(visibilityTimeout, "Visibility timeout name cannot be empty when receving a message");

    return await redis.xautoclaim(
      queueName,
      consumerGroupName,
      consumerName,
      visibilityTimeout,
      "0-0",
      { count: 1 }
    );
  }

  private async readAndVerifyPendingMessage<TStreamResult>(
    blockTimeMs: number
  ): Promise<ParsedStreamMessage<TStreamResult>> {
    const { autoVerify } = this.config;

    const parsedXreadMessage = await this.readAndParseMessage<TStreamResult>(blockTimeMs);

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
        ? await this.receiveBlockingMessage(blockTimeMs, consumerName)
        : await this.receiveNonBlockingMessage(consumerName);

    return parseXreadGroupResponse<StreamResult>(xreadRes);
  }

  private async receiveBlockingMessage(blockTimeMs: number, consumerName: string) {
    const { redis, consumerGroupName, queueName } = this.config;
    invariant(consumerGroupName, "Consumer group name cannot be empty when receiving a message");
    invariant(queueName, "Queue name cannot be empty when receving a message");

    return redis.xreadgroup(consumerGroupName, consumerName, queueName, ">", {
      count: 1,
      blockMS: blockTimeMs,
    });
  }

  private async receiveNonBlockingMessage(consumerName: string) {
    const { redis, consumerGroupName, queueName } = this.config;
    invariant(consumerGroupName, "Consumer group name cannot be empty when receiving a message");
    invariant(queueName, "Queue name cannot be empty when receving a message");

    const data = await redis.xreadgroup(consumerGroupName, consumerName, queueName, ">", {
      count: 1,
    });
    return data;
  }

  async verifyMessage(streamId: string): Promise<"VERIFIED" | "NOT VERIFIED"> {
    const { redis } = this.config;
    await this.initializeConsumerGroup();
    this.decrementConcurrencyCount();

    try {
      invariant(
        this.config.consumerGroupName,
        "Consumer group name cannot be empty when verifying a message"
      );

      invariant(this.config.queueName, "Queue name cannot be empty when verifying a message");

      const xackRes = await redis.xack(
        this.config.queueName,
        this.config.consumerGroupName,
        streamId
      );
      if (typeof xackRes === "number" && xackRes > 0) {
        return "VERIFIED";
      }
      return "NOT VERIFIED";
    } catch (finalError) {
      console.error(
        `Final attempt to acknowledge message failed: ${(finalError as Error).message}`
      );
      return "NOT VERIFIED";
    }
  }

  private generateRandomConsumerName = () => {
    if (this.concurrencyCounter > MAX_CONCURRENCY_LIMIT) {
      throw new Error(ERROR_MAP.CONCURRENCY_LIMIT_EXCEEDED);
    }
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
