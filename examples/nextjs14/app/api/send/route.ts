import { Queue } from "../../../../../src";
import { Redis } from "@upstash/redis";

const redis = new Redis({
  url: "https://fit-ape-37405.upstash.io",
  token:
    "AZIdASQgYTU1NjE1MTQtYjIxNS00NDIwLWJlYTItYWYzOGNmNGYwMWE5ZDA0MDExY2UxOGFjNDI2ZGE4MzliYjQyY2ZkNTRkZjI=",
});

export async function POST(req: Request, res: Response) {
  const { queueName, message } = await req.json();

  const queue = new Queue({
    redis: redis,
    concurrencyLimit: 5,
    queueName: queueName,
  });

  const messageId = await queue.sendMessage({ message: message });

  return Response.json({ messageId });
}
