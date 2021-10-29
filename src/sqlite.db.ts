import {
  BaseCommonDB,
  CommonDB,
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
} from '@naturalcycles/db-lib'
import { JsonSchemaObject, ObjectWithId } from '@naturalcycles/js-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { boldWhite } from '@naturalcycles/nodejs-lib/dist/colors'
import { Database, open } from 'sqlite'
import { OPEN_CREATE, OPEN_READWRITE } from 'sqlite3'
import * as sqlite3 from 'sqlite3'
import { insertSQL } from './query.util'

export interface SQLiteDBCfg {
  filename: string

  /**
   * @default OPEN_READWRITE | OPEN_CREATE
   */
  mode?: number

  /**
   * @default sqlite.Database
   */
  driver?: any
}

const log = Debug('nc:sqlite')

export class SQLiteDB extends BaseCommonDB implements CommonDB {
  constructor(public cfg: SQLiteDBCfg) {
    super()
  }

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

  override async ping(): Promise<void> {
    await this.open()
  }

  override async getTables(): Promise<string[]> {
    const r = await this.db.all('select * from sqlite_master')
    console.log(r)
    return []
  }

  override async createTable<ROW extends ObjectWithId>(
    table: string,
    schema: JsonSchemaObject<ROW>,
    opt: CommonDBCreateOptions = {},
  ): Promise<void> {
    if (opt.dropIfExists) await this.dropTable(table)

    // todo: more proper schema mapping implementation is recommended
    // const sql = commonSchemaToMySQLDDL(schema)
    const sql = `create table ${table} (${Object.keys(schema.properties)
      .map(k => `${k} TEXT`)
      .join(', ')})`
    console.log(sql)
    await this.db.exec(sql)
  }

  /**
   * Use with caution!
   */
  async dropTable(table: string): Promise<void> {
    await this.db.exec(`DROP TABLE IF EXISTS ${table}`)
  }

  override async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    _opt?: CommonDBSaveOptions,
  ): Promise<void> {
    if (!rows.length) return

    const [sql, params] = insertSQL(table, rows)[0]!
    console.log(sql, ...params)
    await this.db.run(sql, ...params)
    // await this.db.exec(`INSERT INTO ${table} VALUES ("test")`)

    // INSERT INTO tbl VALUES ("test")
  }

  override async getByIds<ROW extends ObjectWithId>(
    _table: string,
    _ids: string[],
    _opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    // todo: implement all query.util from mysql
    return []
  }
}
