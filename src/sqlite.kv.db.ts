import { CommonDBCreateOptions, CommonKVDB } from '@naturalcycles/db-lib'
import { pMap, StringMap } from '@naturalcycles/js-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { boldWhite } from '@naturalcycles/nodejs-lib/dist/colors'
import { Database, open } from 'sqlite'
import * as sqlite3 from 'sqlite3'
import { OPEN_CREATE, OPEN_READWRITE } from 'sqlite3'
import { deleteByIdsSQL, insertKVSQL, selectKVSQL } from './query.util'

export interface SQLiteKVDBCfg {
  filename: string

  /**
   * @default OPEN_READWRITE | OPEN_CREATE
   */
  mode?: number

  /**
   * @default sqlite.Database
   */
  driver?: any

  /**
   * @default false
   */
  debug?: boolean
}

interface KVObject {
  id: string
  v: Buffer
}

const log = Debug('nc:sqlite')

export class SQLiteKVDB implements CommonKVDB {
  constructor(public cfg: SQLiteKVDBCfg) {}

  _db?: Database

  get db(): Database {
    if (!this._db) throw new Error('await SQLiteDB.open() should be called before using the DB')
    return this._db
  }

  async open(): Promise<void> {
    if (this._db) return

    this._db = await open({
      driver: sqlite3.Database,
      // eslint-disable-next-line no-bitwise
      mode: OPEN_READWRITE | OPEN_CREATE, // tslint:disable-line
      ...this.cfg,
    })
    log(`${boldWhite(this.cfg.filename)} opened`)
  }

  async close(): Promise<void> {
    if (!this._db) return
    await this.db.close()
    log(`${boldWhite(this.cfg.filename)} closed`)
  }

  async ping(): Promise<void> {
    await this.open()
  }

  async createTable(table: string, opt: CommonDBCreateOptions = {}): Promise<void> {
    if (opt.dropIfExists) await this.dropTable(table)

    const sql = `create table ${table} (id TEXT PRIMARY KEY, v BLOB NOT NULL)`
    console.log(sql)
    await this.db.exec(sql)
  }

  /**
   * Use with caution!
   */
  async dropTable(table: string): Promise<void> {
    await this.db.exec(`DROP TABLE IF EXISTS ${table}`)
  }

  async deleteByIds(table: string, ids: string[]): Promise<void> {
    const sql = deleteByIdsSQL(table, ids)
    if (this.cfg.debug) console.log(sql)
    await this.db.run(sql)
  }

  async getByIds(table: string, ids: string[]): Promise<Buffer[]> {
    const sql = selectKVSQL(table, ids)
    if (this.cfg.debug) console.log(sql)
    const rows = await this.db.all<KVObject[]>(sql)
    // console.log(rows)
    return rows.map(r => r.v)
  }

  async saveBatch(table: string, batch: StringMap<Buffer>): Promise<void> {
    // todo: speedup
    const statements = insertKVSQL(table, batch)

    // if (statements.length > 1) await this.db.run('BEGIN TRANSACTION')

    await pMap(statements, async statement => {
      const [sql, params] = statement
      if (this.cfg.debug) console.log(sql)
      await this.db.run(sql, ...params)
    })

    // if (statements.length > 1) await this.db.run('END TRANSACTION')
  }

  async beginTransaction(): Promise<void> {
    await this.db.run(`BEGIN TRANSACTION`)
  }

  async endTransaction(): Promise<void> {
    await this.db.run(`END TRANSACTION`)
  }
}
