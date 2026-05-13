// Caller 4: session-utility module. This is the caller the planner's v1 spec
// misses. Without write-side coherence, the executor stays on v1, renames the
// other three call sites, and leaves this file referencing the deleted symbol.
import { validateUser, UserCredentials } from "../auth";

export function isSessionValid(creds: UserCredentials): boolean {
  return validateUser(creds);
}
