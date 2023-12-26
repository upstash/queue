import { expect, test } from "bun:test";
import { retryWithBackoff } from "./utils";

test(
  "should test exponential backoff",
  async () => {
    let retryCount = 0;

    async function fetchData() {
      // Simulating an async operation (e.g., fetching data from an API)
      return new Promise((resolve, reject) => {
        // Mocking a fetch call that might fail
        if (Math.random() > 0.5) {
          resolve("Data fetched successfully");
        } else {
          retryCount++;
          reject(new Error("Failed to fetch data"));
        }
      });
    }

    await retryWithBackoff(fetchData, 5, 1000);
    expect(retryCount).toBeGreaterThanOrEqual(1);
  },
  { timeout: 10000 }
);
