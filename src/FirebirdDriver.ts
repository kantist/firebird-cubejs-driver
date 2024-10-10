import { BaseDriver, QueryOptions } from "@cubejs-backend/base-driver";
import { ConnectionPool, Options, pool } from "node-firebird";
import { FirebirdQuery } from "./FirebirdQuery";

export class FirebirdDriver extends BaseDriver {

	private pool: ConnectionPool;

	constructor(
		config: Options & {
			maxPoolSize?: number
		}
	) {
		super();
		this.pool = pool(config.maxPoolSize || 8, config);
	}

	public static dialectClass() {
		return FirebirdQuery;
	}

	

	async testConnection(): Promise<void> {
		return new Promise((resolve, reject) => {
			this.pool.get((err, db) => {
				if (err)
					return reject(err);

				db.query("SELECT 1 FROM RDB$DATABASE", [], (err, result) => {
					db.detach((detachErr) => {
						if (detachErr)
							console.error("Error when detaching from database!", detachErr);
					});

					if (err)
						return reject(err);

					return resolve();
				})
			})
		})
	}

	async query<R = unknown>(query: string, values: unknown[], options?: QueryOptions): Promise<R[]> {
		return new Promise((resolve, reject) => {
			this.pool.get((err, db) => {
				if (err)
					return reject(err);

				db.query(query, values, (err, result) => {
					db.detach((detachErr) => {
						if (detachErr)
							console.error("Error when detaching from database!", detachErr);
					});

					if (err)
						return reject(err);

					return resolve(result);
				})
			})
		});
	}

	protected informationSchemaQuery(): string {
		return `SELECT
			TRIM(r.RDB$RELATION_NAME) AS "table_name",
			TRIM(f.RDB$FIELD_NAME) AS "column_name",
			TRIM(r.RDB$OWNER_NAME) AS "table_schema",
			TRIM(t.RDB$TYPE_NAME) AS "data_type"
		FROM RDB$RELATION_FIELDS f
		JOIN RDB$RELATIONS r ON f.RDB$RELATION_NAME = r.RDB$RELATION_NAME
		JOIN RDB$FIELDS fld ON f.RDB$FIELD_SOURCE = fld.RDB$FIELD_NAME
		JOIN RDB$TYPES t ON fld.RDB$FIELD_TYPE = t.RDB$TYPE
		WHERE r.RDB$SYSTEM_FLAG = 0
		AND r.RDB$RELATION_NAME NOT IN ('RDB$DATABASE', 'RDB$FIELDS', 'RDB$INDEX_SEGMENTS', 'RDB$INDICES', 'RDB$RELATION_CONSTRAINTS', 'RDB$RELATION_FIELDS', 'RDB$ROLES', 'RDB$SECURITY_CLASSES', 'RDB$TRIGGERS', 'RDB$USER_PRIVILEGES', 'RDB$VIEW_RELATIONS')
		AND t.RDB$FIELD_NAME = 'RDB$FIELD_TYPE'`
	}

	protected primaryKeysQuery(conditionString?: string): string | null {
		return `SELECT 
			TRIM(r.RDB$OWNER_NAME) AS "table_schema",
			TRIM(cst.RDB$RELATION_NAME) AS "table_name",
			TRIM(s.RDB$FIELD_NAME) AS "column_name"
		FROM RDB$RELATION_CONSTRAINTS cst
		JOIN RDB$RELATIONS r ON cst.RDB$RELATION_NAME = r.RDB$RELATION_NAME
		JOIN RDB$INDEX_SEGMENTS s ON cst.RDB$INDEX_NAME = s.RDB$INDEX_NAME
		WHERE cst.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
		AND cst.RDB$RELATION_NAME NOT IN ('RDB$DATABASE')${conditionString ? ` AND (${conditionString})` : ''}`;
	}

	protected foreignKeysQuery(conditionString?: string): string | null {
		return `SELECT
			TRIM(r.RDB$OWNER_NAME) AS "table_schema",
			TRIM(rc.RDB$RELATION_NAME) AS "table_name",
			TRIM(isc.RDB$FIELD_NAME) AS "column_name",
			TRIM(refc.RDB$CONST_NAME_UQ) AS "target_table",
			TRIM(isf.RDB$FIELD_NAME) AS "target_column"
		FROM RDB$RELATION_CONSTRAINTS rc
		JOIN RDB$RELATIONS r ON rc.RDB$RELATION_NAME = r.RDB$RELATION_NAME
		JOIN RDB$INDEX_SEGMENTS isc ON rc.RDB$INDEX_NAME = isc.RDB$INDEX_NAME
		JOIN RDB$REF_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
		JOIN RDB$RELATION_CONSTRAINTS tgtc ON tgtc.RDB$CONSTRAINT_NAME = refc.RDB$CONST_NAME_UQ
		JOIN RDB$INDEX_SEGMENTS isf ON tgtc.RDB$INDEX_NAME = isf.RDB$INDEX_NAME
		WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
		AND rc.RDB$RELATION_NAME NOT IN ('RDB$DATABASE')${conditionString ? ` AND (${conditionString})` : ''}`;
	}

	/**
	 * Not being called by cube but added it just in case
	 */
	public wrapQueryWithLimit(query: { query: string, limit: number }) {
		query.query = `SELECT * FROM (${query.query}) AS t ROWS 1 TO ${query.limit}`;
	}

	async release(): Promise<void> {
		if (this.pool)
			this.pool.destroy();
	}
}