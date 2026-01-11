import { describe, it, expect } from "vitest";
import {
  EVENT_DISCRIMINATORS,
  DISCRIMINATOR_TO_EVENT,
} from "../../../src/parser/types.js";

describe("Parser Types", () => {
  describe("EVENT_DISCRIMINATORS", () => {
    it("should contain all 14 event discriminators", () => {
      const eventNames = Object.keys(EVENT_DISCRIMINATORS);
      expect(eventNames).toHaveLength(14);
    });

    it("should have correct event names", () => {
      const expectedEvents = [
        "AgentOwnerSynced",
        "AgentRegisteredInRegistry",
        "BaseRegistryCreated",
        "BaseRegistryRotated",
        "FeedbackRevoked",
        "MetadataDeleted",
        "MetadataSet",
        "NewFeedback",
        "ResponseAppended",
        "UriUpdated",
        "UserRegistryCreated",
        "ValidationRequested",
        "ValidationResponded",
        "WalletUpdated",
      ];

      for (const event of expectedEvents) {
        expect(EVENT_DISCRIMINATORS).toHaveProperty(event);
      }
    });

    it("should have 16-character hex discriminators", () => {
      for (const [name, disc] of Object.entries(EVENT_DISCRIMINATORS)) {
        expect(disc).toMatch(/^[0-9a-f]{16}$/);
      }
    });

    it("should have unique discriminators", () => {
      const discriminators = Object.values(EVENT_DISCRIMINATORS);
      const uniqueDiscriminators = new Set(discriminators);
      expect(uniqueDiscriminators.size).toBe(discriminators.length);
    });
  });

  describe("DISCRIMINATOR_TO_EVENT", () => {
    it("should be reverse mapping of EVENT_DISCRIMINATORS", () => {
      for (const [name, disc] of Object.entries(EVENT_DISCRIMINATORS)) {
        expect(DISCRIMINATOR_TO_EVENT[disc]).toBe(name);
      }
    });

    it("should have same length as EVENT_DISCRIMINATORS", () => {
      expect(Object.keys(DISCRIMINATOR_TO_EVENT).length).toBe(
        Object.keys(EVENT_DISCRIMINATORS).length
      );
    });
  });

  describe("Event discriminator values", () => {
    it("AgentRegisteredInRegistry should have correct discriminator", () => {
      expect(EVENT_DISCRIMINATORS.AgentRegisteredInRegistry).toBe(
        "ebf157e201dfbaaf"
      );
    });

    it("NewFeedback should have correct discriminator", () => {
      expect(EVENT_DISCRIMINATORS.NewFeedback).toBe("0ea23ac2832a0b95");
    });

    it("ValidationRequested should have correct discriminator", () => {
      expect(EVENT_DISCRIMINATORS.ValidationRequested).toBe("852afcc65287b741");
    });
  });
});
