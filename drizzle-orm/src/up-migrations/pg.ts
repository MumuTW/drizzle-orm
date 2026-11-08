import type { BatchItem } from '~/batch.ts';
import type { MigrationMeta } from '~/migrator.ts';
import type { PgAsyncDatabase } from '~/pg-core/async/db.ts';
import type { PgQueryResultHKT } from '~/pg-core/session.ts';
import { type SQL, sql } from '~/sql/sql.ts';
import { assertUnreachable } from '~/utils.ts';
import { GET_VERSION_FOR, type UpgradeResult } from './utils.ts';

/**
 * Upgrade from version 0 to version 1:
 * Validate a complete, unambiguous, one-to-one mapping of historical SQL before
 * backfilling names. Folder timestamps can change during conversion and are not
 * evidence of identity. Identical SQL in multiple local migrations is ambiguous.
 */
const upToV1 = async (
	migrationsSchema: string,
	migrationsTable: string,
	db: PgAsyncDatabase<PgQueryResultHKT, any>,
	localMigrations: MigrationMeta[],
	mode: 'batch' | 'execute',
): Promise<void> => {
	const table = sql`${sql.identifier(migrationsSchema)}.${sql.identifier(migrationsTable)}`;

	// 1. Read all existing DB migrations
	// Sort them by ids asc (order how they were applied)
	const dbRows = await db.session.objects<{ id: number; hash: string }>(
		sql`SELECT id, hash FROM ${table} ORDER BY id ASC`,
	);

	// Keep all candidates: a hash checks content, but is not a unique migration ID.
	const byHash = new Map<string, MigrationMeta[]>();
	for (const lm of localMigrations) {
		const candidates = byHash.get(lm.hash) ?? [];
		candidates.push(lm);
		byHash.set(lm.hash, candidates);
	}

	const toApply: { id: number; name: string }[] = [];
	const matchedNames = new Map<string, number>();
	const problems: string[] = [];

	for (const dbRow of dbRows) {
		const candidates = byHash.get(dbRow.hash);
		if (!candidates?.length) {
			problems.push(`id ${dbRow.id}: no local migration has the stored SQL hash`);
			continue;
		}
		if (candidates.length !== 1) {
			problems.push(
				`id ${dbRow.id}: multiple local migrations have the stored SQL hash (${
					candidates.map((c) => c.name).join(', ')
				})`,
			);
			continue;
		}

		const matched = candidates[0]!;
		const previousId = matchedNames.get(matched.name);
		if (previousId !== undefined) {
			problems.push(`ids ${previousId}, ${dbRow.id}: both match local migration ${matched.name}`);
			continue;
		}
		matchedNames.set(matched.name, dbRow.id);
		toApply.push({ id: dbRow.id, name: matched.name });
	}

	// A partial plan must never change the ledger or make old SQL look pending.
	if (problems.length > 0) {
		throw Error(
			`Cannot safely upgrade your database migrations table: ${
				problems.join('; ')
			}. Resolve the missing, modified or ambiguous migration history before retrying. No migration names have been written.`,
		);
	}

	// 5. Create extra column and backfill names for matched migrations
	const sqls: SQL[] = [
		sql`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${sql.identifier('name')} text`,
		sql`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${
			sql.identifier('applied_at')
		} timestamp with time zone DEFAULT now()`,
	];
	for (const { id, name } of toApply) {
		sqls.push(
			sql`UPDATE ${table} SET ${sql.identifier('name')} = ${name}, ${sql.identifier('applied_at')} = NULL WHERE ${
				sql.identifier('id')
			} = ${id}`,
		);
	}

	// batch - neon http
	// execute - xata http
	// Transactional callers already hold the ledger lock in the enclosing transaction.
	if (mode === 'batch') {
		const database = db as PgAsyncDatabase<PgQueryResultHKT, any> & {
			batch(batch: [BatchItem<'pg'>, ...BatchItem<'pg'>[]]): Promise<unknown>;
		};

		await database.batch(
			sqls.map((s) => database.execute(s)) as unknown as [BatchItem<'pg'>, ...BatchItem<'pg'>[]],
		);
	} else if (mode === 'execute') {
		for (const sql of sqls) {
			await db.session.execute(sql);
		}
	} else {
		assertUnreachable(mode);
	}
};

/**
 * Detects the current version of the migrations table schema and upgrades it if needed.
 *
 * Version 0: Original schema (id, hash, created_at)
 * Version 1: Extended schema (id, hash, created_at, name, applied_at)
 */
export async function upgradeIfNeeded(
	migrationsSchema: string,
	migrationsTable: string,
	db: PgAsyncDatabase<PgQueryResultHKT, any>,
	localMigrations: MigrationMeta[],
	// batch - neon http
	// execute - xata http
	// transaction - all others
	mode: 'transaction' | 'batch' | 'execute' = 'transaction',
): Promise<UpgradeResult> {
	// Check if the table exists at all
	const result = await db.session.objects<{ '1': 1 }>(
		sql`SELECT 1 FROM information_schema.tables
			WHERE table_schema = ${migrationsSchema}
			AND table_name = ${migrationsTable}`,
	);

	if (result.length === 0) {
		return { newDb: true };
	}

	// Table exists, check table shape
	const rows = await db.session.objects<
		{ schema: string; table_name: string; column_name: string; type: string }
	>(
		sql`SELECT
			n.nspname AS "schema",
			c.relname AS "table_name",
			a.attname AS "column_name",
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS "type"
		FROM
			pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE
			a.attnum > 0
			AND NOT a.attisdropped
			AND n.nspname = ${migrationsSchema}
			AND c.relname = ${migrationsTable}
		ORDER BY a.attnum;`,
	);

	const version = GET_VERSION_FOR.pg(rows.map((r) => r.column_name));

	if (version < 1) {
		if (mode === 'transaction') {
			await db.transaction(async (tx) => {
				// Keep the historical rows stable between validation and backfill. A second
				// upgrader must recheck the version after acquiring the lock, not remap v1.
				const table = sql`${sql.identifier(migrationsSchema)}.${sql.identifier(migrationsTable)}`;
				await tx.execute(sql`LOCK TABLE ${table} IN ACCESS EXCLUSIVE MODE`);
				await upgradeIfNeeded(migrationsSchema, migrationsTable, tx, localMigrations, 'execute');
			});
		} else {
			await upToV1(migrationsSchema, migrationsTable, db, localMigrations, mode);
		}
	}

	return { newDb: false };
}
