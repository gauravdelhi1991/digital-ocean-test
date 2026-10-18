import { describe, expect, it } from "vitest";
import { createDatabasePool } from "../src/database";

describe("database connection pool", () => {
  it("uses TLS without CA verification for sslmode=require URLs", async () => {
    const pool = createDatabasePool(
      "postgres://user:password@db.example.test/jobs?sslmode=require",
    );

    expect(pool.options.connectionString).toBe(
      "postgres://user:password@db.example.test/jobs?sslmode=require&uselibpqcompat=true",
    );
    await pool.end();
  });

  it("preserves explicit SSL compatibility settings", async () => {
    const pool = createDatabasePool(
      "postgres://user:password@db.example.test/jobs?sslmode=require&uselibpqcompat=false",
    );

    expect(pool.options.connectionString).toBe(
      "postgres://user:password@db.example.test/jobs?sslmode=require&uselibpqcompat=false",
    );
    await pool.end();
  });
});
