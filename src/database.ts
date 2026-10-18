import { Pool } from "pg";

export function createDatabasePool(connectionString: string): Pool {
  const url = new URL(connectionString);
  if (
    url.searchParams.get("sslmode")?.toLowerCase() === "require" &&
    !url.searchParams.has("uselibpqcompat")
  ) {
    // DigitalOcean's managed database uses a self-signed CA; require TLS without CA verification.
    url.searchParams.set("uselibpqcompat", "true");
  }

  return new Pool({ connectionString: url.toString() });
}
