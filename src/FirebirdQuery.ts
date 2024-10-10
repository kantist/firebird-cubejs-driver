import { BaseFilter, BaseQuery, ParamAllocator } from "@cubejs-backend/schema-compiler";

class FirebirdParamAllocator extends ParamAllocator {
	public paramPlaceHolder(paramIndex) {
		return `?`;
	}
}

class FirebirdFilter extends BaseFilter {

	/**
	 * "ILIKE" does't support
	 */
	public likeIgnoreCase(column, not, param, type) {
		const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
		const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
		return `${column}${not ? ' NOT' : ''} SIMILAR TO ${p}${this.allocateParam(param)}${s}`;
	}
}

export class FirebirdQuery extends BaseQuery {

	public sqlTemplates() {
		const templates = super.sqlTemplates();
		templates.params.param = '?';
		return templates;
	}

	public newParamAllocator(expressionParams) {
		return new FirebirdParamAllocator(expressionParams);
	}

	protected limitOffsetClause(limit: any, offset: any): string {
		if((!offset || offset == 0) && !limit)
			return ``;

		if(!offset || offset == 0)
			offset = 1;

		if(!limit)
			limit = 10000;

		return ` ROWS ${offset} TO ${offset + limit} `;
	}

	public castToString(sql: any): string {
		return `CAST(${sql} as VARCHAR(32767))`;
	}

	//@ts-ignore
	public newFilter(filter) {
		//@ts-ignore
		return new FirebirdFilter(this, filter);
	}
}