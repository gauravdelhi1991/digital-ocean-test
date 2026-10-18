import { describe, expect, it } from "vitest";
import { parseRedisConnectionOptions } from "../src/job-queue";

describe("Redis connection configuration", () => {
  it("parses managed TLS Redis credentials and port", () => {
    expect(
      parseRedisConnectionOptions(
        "rediss://queue-user:secret%40value@redis.example.test:25061",
      ),
    ).toMatchObject({
      host: "redis.example.test",
      port: 25061,
      username: "queue-user",
      password: "secret@value",
      db: 0,
      tls: {},
      maxRetriesPerRequest: 1,
    });
  });

  it("rejects unsupported schemes and non-numeric database paths", () => {
    expect(() => parseRedisConnectionOptions("http://redis.example.test")).toThrow(
      "REDIS_URL must use redis:// or rediss://",
    );
    expect(() =>
      parseRedisConnectionOptions("redis://redis.example.test/defaultdb"),
    ).toThrow("REDIS_URL path must be a non-negative database number");
  });

  it("enables TLS when the managed database URI requests sslmode=require", () => {
    expect(
      parseRedisConnectionOptions("redis://redis.example.test:25061?sslmode=require"),
    ).toMatchObject({ port: 25061, tls: {} });
  });
});
