import { Pool, type QueryResultRow } from "pg"

const DEFAULT_DB_URL = "postgresql://rs2:rs2@localhost:5432/rs2_dashboard"

declare global {
  // eslint-disable-next-line no-var
  var __rs2Pool: Pool | undefined
}

function getPool(): Pool {
  if (!global.__rs2Pool) {
    global.__rs2Pool = new Pool({
      connectionString: process.env.DATABASE_URL || DEFAULT_DB_URL,
      max: 10,
      idleTimeoutMillis: 30000,
    })
  }
  return global.__rs2Pool
}

export async function dbQuery<T extends QueryResultRow>(text: string, params: unknown[] = []): Promise<T[]> {
  const pool = getPool()
  const result = await pool.query<T>(text, params)
  return result.rows
}
