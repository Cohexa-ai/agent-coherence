// Caller 1: request-middleware path.
import { validateUser, UserCredentials } from "./auth";

export function authMiddleware(creds: UserCredentials): void {
  if (!validateUser(creds)) {
    throw new Error("unauthenticated");
  }
}
