import { describe, expect, it } from "vitest";
import { queueDepthFromMetrics } from "../src/queue-operations";

describe("queue depth", () => {
  it("combines persisted job metrics with BullMQ queue states", () => {
    expect(
      queueDepthFromMetrics(
        {
          accepted: 2,
          running: 1,
          deadLettered: 4,
          backlogLagSeconds: 15.5,
        },
        {
          waiting: 3,
          paused: 5,
          prioritized: 7,
          active: 2,
          delayed: 6,
        },
        true,
      ),
    ).toEqual({
      accepted: 2,
      queued: 15,
      running: 2,
      delayed: 6,
      deadLettered: 4,
      backlogLagSeconds: 15.5,
      draining: true,
    });
  });

  it("reports no backlog lag when the database has no pending work", () => {
    expect(
      queueDepthFromMetrics(
        {
          accepted: 0,
          running: 0,
          deadLettered: 0,
          backlogLagSeconds: null,
        },
        {},
        false,
      ).backlogLagSeconds,
    ).toBeNull();
  });
});
