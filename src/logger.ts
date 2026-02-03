import pino from "pino";
import { config } from "./config.js";

const isDevelopment = process.env.NODE_ENV !== "production";

export const logger = pino({
  level: config.logLevel,
  ...(isDevelopment && {
    transport: {
      target: "pino-pretty",
      options: {
        colorize: true,
        translateTime: "SYS:standard",
        ignore: "pid,hostname",
      },
    },
  }),
});

export function createChildLogger(name: string) {
  return logger.child({ component: name });
}
