// Caller 3: refresh-token route handler.
import { validateUser, UserCredentials } from "../auth";

export function handleRefresh(creds: UserCredentials): boolean {
  return validateUser(creds);
}
