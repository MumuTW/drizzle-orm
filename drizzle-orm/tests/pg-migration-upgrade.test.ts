import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, expect, test } from 'vitest';
import { readMigrationFiles } from '~/migrator';
import { drizzle } from '~/pglite';
import { migrate } from '~/pglite/migrator';
import { sql } from '~/sql';
import { upgradeIfNeeded } from '~/up-migrations/pg';

let db: ReturnType<typeof drizzle>;
let folder: string;
const time = Date.UTC(2026, 0, 1);
const first = '20260101000000_a';
const second = '20260101000001_b';
const increment = 'UPDATE counter SET n = n + 1;';

function migration(name: string, query: string) {
	mkdirSync(join(folder, name), { recursive: true });
	writeFileSync(join(folder, name, 'migration.sql'), query);
}

beforeEach(async () => {
	folder = mkdtempSync(join(tmpdir(), 'drizzle-pg-upgrade-test-'));
	db = drizzle('memory://');
	await db.execute(sql`CREATE SCHEMA drizzle`);
	await db.execute(sql`CREATE TABLE drizzle.__drizzle_migrations (
		id serial PRIMARY KEY, hash text NOT NULL, created_at bigint
	)`);
	await db.execute(sql`CREATE TABLE counter (n integer NOT NULL)`);
	await db.execute(sql`INSERT INTO counter VALUES (1)`);
});

afterEach(async () => {
	await db.$client.close();
	rmSync(folder, { recursive: true, force: true });
});

async function history(names = [first, second], times = [time + 100, time + 200], createTable = false) {
	migration(names[0]!, 'CREATE TABLE counter (n integer NOT NULL);');
	migration(
		names[1]!,
		increment + (createTable ? '\n--> statement-breakpoint\nCREATE TABLE applied (id integer);' : ''),
	);
	if (createTable) await db.execute(sql`CREATE TABLE applied (id integer)`);
	const local = readMigrationFiles({ migrationsFolder: folder });
	for (const [index, entry] of local.entries()) {
		await db.execute(sql`INSERT INTO drizzle.__drizzle_migrations (hash, created_at)
			VALUES (${entry.hash}, ${times[index]})`);
	}
	return local;
}

async function state() {
	return {
		columns: await db.session.objects(sql`SELECT column_name FROM information_schema.columns
			WHERE table_schema = 'drizzle' AND table_name = '__drizzle_migrations' ORDER BY ordinal_position`),
		rows: await db.session.objects(sql`SELECT * FROM drizzle.__drizzle_migrations ORDER BY id`),
		counter: await db.session.objects(sql`SELECT n FROM counter`),
	};
}

test.each([
	{ label: 'same-second history', name: '20260101000000_b', applied: time + 200, createTable: false },
	{ label: 'separate-second history', name: second, applied: time + 1200, createTable: false },
	{ label: 'retimestamped DML', name: second, applied: time + 200, createTable: false },
	{ label: 'retimestamped DDL', name: second, applied: time + 200, createTable: true },
])('preserves already-applied $label without replay', async ({ name, applied, createTable }) => {
	const local = await history([first, name], [time + 100, applied], createTable);
	await migrate(db, { migrationsFolder: folder });
	await migrate(db, { migrationsFolder: folder });
	expect(await db.session.objects(sql`SELECT hash, name FROM drizzle.__drizzle_migrations ORDER BY id`))
		.toEqual(local.map(({ hash, name }) => ({ hash, name })));
	expect(await db.session.objects(sql`SELECT n FROM counter`)).toEqual([{ n: 1 }]);
});

test('applies genuinely pending migrations after validating all history', async () => {
	await history();
	migration('20260101000002_c', 'UPDATE counter SET n = n + 10;');
	await migrate(db, { migrationsFolder: folder });
	expect(await db.session.objects(sql`SELECT n FROM counter`)).toEqual([{ n: 11 }]);
	expect((await state()).rows).toHaveLength(3);
});

test.each(['changed SQL', 'missing SQL', 'ambiguous SQL', 'reused identity'])(
	'refuses %s before changing ledger schema, rows or data',
	async (problem) => {
		const local = await history();
		if (problem === 'changed SQL') migration(second, 'UPDATE counter SET n = n + 100;');
		if (problem === 'missing SQL') rmSync(join(folder, second), { recursive: true });
		if (problem === 'ambiguous SQL') migration('20260101000002_c', increment);
		if (problem === 'reused identity') {
			await db.execute(sql`INSERT INTO drizzle.__drizzle_migrations (hash, created_at)
				VALUES (${local[1]!.hash}, ${time + 300})`);
		}
		const before = await state();
		await expect(migrate(db, { migrationsFolder: folder })).rejects.toThrow(/Cannot safely upgrade/);
		expect(await state()).toEqual(before);
	},
);

test.each(['batch', 'execute'] as const)('validates before any writes in %s mode', async (mode) => {
	await history();
	migration(second, 'UPDATE counter SET n = n + 100;');
	const before = await state();
	await expect(
		upgradeIfNeeded('drizzle', '__drizzle_migrations', db, readMigrationFiles({ migrationsFolder: folder }), mode),
	)
		.rejects.toThrow(/Cannot safely upgrade/);
	expect(await state()).toEqual(before);
});

test('rolls back schema and all names when a backfill write fails', async () => {
	await history();
	await db.execute(sql`CREATE FUNCTION reject_backfill() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.id = 2 THEN RAISE EXCEPTION 'backfill rejected'; END IF; RETURN NEW; END;
	$$`);
	await db.execute(sql`CREATE TRIGGER reject_backfill BEFORE UPDATE ON drizzle.__drizzle_migrations
		FOR EACH ROW EXECUTE FUNCTION reject_backfill()`);
	const before = await state();
	await expect(migrate(db, { migrationsFolder: folder })).rejects.toThrow();
	expect(await state()).toEqual(before);
});
