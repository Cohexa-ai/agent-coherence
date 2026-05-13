// Caller 2: login route handler.
import { validateUser, UserCredentials } from "../auth";

export function handleLogin(creds: UserCredentials): string {
  if (validateUser(creds)) {
    return "ok";
  }
  return "denied";
}
