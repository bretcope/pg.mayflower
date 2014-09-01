"use strict";
/* -------------------------------------------------------------------
 * Require Statements << Keep in alphabetical order >>
 * ---------------------------------------------------------------- */

var Crypto = require('crypto');
var Fs = require('fs');
var Path = require('path');
var Pg = require('co-pg')(require('pg.js'));
var thunkify = require('thunkify');

var readDir = thunkify(Fs.readdir);
var readFile = thunkify(Fs.readFile);

/* =============================================================================
 * 
 * PgMayflower.js
 *  
 * ========================================================================== */

module.exports = PgMayflower;

function PgMayflower(options)
{
	options = options || {};
	
	this.connectionString = options.connectionString || process.env.DATABASE_URL;
	this.directory = options.directory || Path.join(process.cwd(), 'migrations');
	this.table = options.table || 'migrations';
	this.schema = options.schema || 'public';
	this._tableExists = false;
}

/* -------------------------------------------------------------------
 * Getters / Setters
 * ---------------------------------------------------------------- */

Object.defineProperty(PgMayflower.prototype, 'schemaAndTable', {
	get: function () { return this.schema + '.' + this.table; }
});

/* -------------------------------------------------------------------
 * Prototype Methods << Keep in alphabetical order >>
 * ---------------------------------------------------------------- */

PgMayflower.prototype.createMigrationsTable = function * (db, options)
{
	if (this._tableExists)
		return;
	
	var sql = 'SELECT * FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2;';
	var result = yield db.query_(sql, [ this.schema, this.table ]);
	if (result.rowCount > 0)
	{
		this._tableExists = true;
		return;
	}
	
	if (options.preview)
		return;
	
	// create table
	sql = "CREATE TABLE "+ this.schemaAndTable +" (" +
		"   hash character varying(40)," + 
		"   filename character varying(260)," + 
		"   execution_date timestamp with time zone," + 
		"   duration integer," + 
		"   CONSTRAINT pk_migrations PRIMARY KEY (hash)," + 
		"   CONSTRAINT ux_migrations_filename UNIQUE (filename)" +
		");";
	yield db.query_(sql);
	this._tableExists = true;
};

/**
 * @return {SqlScript[]}
 */
PgMayflower.prototype.getAllScripts = function * ()
{
	var sqlExt = /.+\.sql$/;
	var names = [];
	
	var files = yield readDir(this.directory);
	for (let i = 0; i < files.length; i++)
	{
		if (sqlExt.test(files[i]))
			names.push(files[i]);
	}
	
	names.sort();
	var scripts = [];
	/** @type {SqlScript} */
	var s;
	for (let i = 0; i < names.length; i++)
	{
		s = yield this.getScript(names[i]);
		if (s.sql) // filter out empty scripts
			scripts.push(s);
	}
	
	return scripts;
};

/**
 * @param name {string}
 * @return {SqlScript}
 */
PgMayflower.prototype.getScript = function * (name)
{
	var sql = yield readFile(Path.join(this.directory, name), { encoding: 'utf8' });
//	var sql = require('fs').readFileSync(Path.join(this.directory, name), { encoding: 'utf8' });
	return new SqlScript(name, sql);
};

/**
 * @param db
 * @param options {{ preview:boolean, force:boolean }}
 * @param script {SqlScript}
 */
PgMayflower.prototype.migrate = function * (db, options, script)
{
	var res = new MigrationResult(script.name);
	var exists = false;
	
	yield this.createMigrationsTable(db, options);
	
	var sqlResult;
	if (!this._tableExists && options.preview)
	{
		// pretend like the table exists but is empty in preview mode
		sqlResult = { rowCount: 0 };
	}
	else
	{
		// check for previous migration by hash
		sqlResult = yield db.query_('select * from ' + this.schemaAndTable + ' where hash = $1;', [ script.hash ]);
	}
	
	if (sqlResult.rowCount > 0)
	{
		// migration has already been applied
		let r = sqlResult.rows[0];
		if (r.filename !== script.name)
		{
			res.message = 'Filename has changed in the database; updating ' + script.name;
			if (!options.preview)
			{
				let sql = 'update ' + this.schemaAndTable + ' set filename = $1 where hash = $2;';
				yield db.query_(sql, [ script.name, script.hash ]);
			}
		}

		return res;
	}

	// check for previous migration by filename
	if (!this._tableExists && options.preview)
	{
		sqlResult = { rowCount: 0 };
	}
	else
	{
		sqlResult = yield db.query_('select * from ' + this.schemaAndTable + ' where filename = $1;', [ script.name ]);
	}

	if (sqlResult.rowCount > 0)
	{
		if (!options.force)
			throw new Error('Failed to migrate: ' + script.name + ' (Hash: ' + script.hash + ') - the file was already migrated in the past, to force migration use options.force');
		
		exists = true;
	}

	// begin migration transaction
	yield db.query_('BEGIN');
	// start timer
	var start = process.hrtime();
	// run migration
	try
	{
		yield db.query_(script.sql);
		
		// stop timer
		var duration = process.hrtime(start);
		res.runtime = Math.round((duration[0] * 1000) + (duration[1] / 1000000));

		// mark migration as applied in the database
		if (this._tableExists && !options.preview)
		{
			let sql;
			if (exists)
			{
				sql = 'update ' + this.schemaAndTable + ' set hash = $1, execution_date = $2, duration = $3 where filename = $4;';
			}
			else
			{
				sql = 'insert into ' + this.schemaAndTable + ' (hash, execution_date, duration, filename) values ($1, $2, $3, $4);';
			}

			yield db.query_(sql, [ script.hash, new Date(), res.runtime, script.name ]);
		}
		
		yield db.query_(options.preview ? 'ROLLBACK' : 'COMMIT');
		res.skipped = false;
		res.message = 'Successfully migrated ' + script.name;
	}
	catch (ex)
	{
		yield db.query_('ROLLBACK');
		throw ex;
	}

	return res;
};

/**
 * @param options {{ preview:boolean, force:boolean, output:boolean }}
 * @return {MigrationResult[]}
 */
PgMayflower.prototype.migrateAll = function * (options)
{
	//default options
	if (!options || typeof options !== 'object')
	{
		options = {};
	}

	if (options.output === undefined)
		options.output = true;
	
	/** @type {SqlScript[]} */
	var scripts = yield this.getAllScripts();

	var db = new Pg.Client(this.connectionString);
	yield db.connect_();
	
	var error;
	try
	{
		/** @type {MigrationResult[]} */
		var results = [];
		var r;
		for (let i = 0; i < scripts.length; i++)
		{
			r = yield this.migrate(db, options, scripts[i]);
			results.push(r);
		}
	}
	catch (ex)
	{
		error = ex;
	}
	
	db.end();
	
	if (error)
		throw error;
	
	var skipped = 0;
	for (let i = 0; i < results.length; i++)
	{
		if (!results[i])
			continue;

		if (results[i].skipped)
			skipped++;

		if (results[i].message && options.output)
		{
			console.log(results[i].message);
		}
	}

	if (options.output && skipped > 0)
	{
		console.log(skipped + ' previously applied migrations were skipped.');
	}
	
	return results;
};

/* =============================================================================
 * 
 * MigrationResult Class
 *  
 * ========================================================================== */

/**
 * @param name {string}
 * @constructor
 */
function MigrationResult (name)
{
	this.name = name;
	this.skipped = true;
	this.runtime = 0;
	this.message = '';
}

/* =============================================================================
 * 
 * SqlScript Class
 *  
 * ========================================================================== */

/**
 *
 * @param name {string}
 * @param sql {string}
 * @constructor
 */
function SqlScript (name, sql)
{
	this.name = name;
	this.sql = sql.trim();
	this.hash = getHash(sql.replace(/\r\n/g, '\n'));
}

/* -------------------------------------------------------------------
 * Private Helper Methods << Keep in alphabetical order >>
 * ---------------------------------------------------------------- */

/**
 * Returns an md5 hash formatted like a GUID (for compatibility with the C# migrator implementation).
 * @param sql {string}
 * @returns {string}
 */
function getHash (sql)
{
	var md5 = Crypto.createHash('md5');
	md5.update(sql);
	var parts = /^(.{8})(.{4})(.{4})(.{4})(.{12})$/.exec(md5.digest('hex'));
	return parts[1] + '-' + parts[2] + '-' + parts[3] + '-' + parts[4] + '-' + parts[5];
}
