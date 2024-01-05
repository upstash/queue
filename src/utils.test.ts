import { describe, expect, test } from "bun:test";
import { parseXclaimAutoResponse, parseXreadGroupResponse } from "./utils";

describe("Response Parsers", () => {
  test("should parse XCLAIMAUTO response", async () => {
    const input = ["0-0", [["1703754659687-0", ["messageBody", '{"hello":"world"}']]], []];

    expect(parseXclaimAutoResponse(input)).toEqual({
      streamId: "1703754659687-0",
      body: '{"hello":"world"}',
    });
  });

  test("should parse XREADGROUP response", async () => {
    const input = [
      ["UpstashMQ:1251e0e7", [["1703755686316-0", ["messageBody", '{"hello":"world"}']]]],
    ];

    expect(parseXreadGroupResponse(input)).toEqual({
      streamId: "1703755686316-0",
      body: '{"hello":"world"}',
    });
  });
});
