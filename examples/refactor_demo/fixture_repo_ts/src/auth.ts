// Auth module. Defines the symbol the demo's planner asks the executor to rename.
// The demo's task spec v1 lists 3 callers; v2 adds a 4th. With write-side coherence,
// the executor refreshes its cached spec before committing and renames all 4 sites.
// Without coherence, the executor uses the stale v1 spec and misses caller #4 — the
// resulting build error is the demo's visible failure.

export interface UserCredentials {
  email: string;
  passwordHash: string;
}

export function validateUser(creds: UserCredentials): boolean {
  return creds.email.includes("@") && creds.passwordHash.length > 0;
}
