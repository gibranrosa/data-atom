"use babel";

import firebird from 'node-firebird';
import DataManager from './data-manager';

export default class FirebirdManager extends DataManager {
  constructor(dbConfig) {
    super(dbConfig);
    this.pool = null;
  }

  destroy() {
    if (this.pool) {
      this.pool.destroy();
      // ((err) => {
      //   if (err)
      //     console.log(`Error closing the connection pool: ${err}`);
      // });
    }
    this.pool = null;
  }

  execute(database, query, onQueryToken) {
    return new Promise((resolve, reject) => {
      var url = database !== '' ? this.dbConfig.getUrlWithDb(database) : this.dbConfig.getUrl();
      if (!this.pool) {
        let config = {
          host: this.dbConfig.server,
          user: this.dbConfig.user,
          password: this.dbConfig.password,
          port: this.dbConfig.port,
          lowercase_keys: false,
          multipleStatements: true
        };

        if (database) {
          config.database = database;
          this.dbConfig.database = database;
        }
        this.pool = firebird.pool(5,config);
      }

      this.pool.get((err, connection) => {
        if (err)
          return reject(this.translateError(err));

        if (connection) {
          connection.query( query, (err, results, fields) => {
            if (err)
              return reject(this.translateError(err));

            // if (!fields || (fields.length && fields[0].constructor.name === 'FieldPacket')) {}
            //   resolve(this.translateResults([results], [fields]));
            //  else
              resolve(this.translateResults(results, fields));
            //
            connection.detach();
          });
        }
      });
    });
  }

  // turn error into some useful string
  translateError(err) {
    return err.toString(); // dumb for now
  }

  // conver the results into what we expect so the UI doens't have to handle all different result types
  translateResults(results, fieldResults) {
    var translatedResults = [];

    //for (var i = 0; i < results.length; i++) {
      //let rows = results[i];

      // if (this.isOkPacket(rows)) {}
      //   let message = rows.message ? rows.message.toString() : `${rows.affectedRows} rows affected. ${rows.changedRows} changed.`;
      //
      //   translatedResults.push({})
      //     message: message,
      //     fields: [],
      //     rowCount: rows.affectedRows,
      //     rows: []
      //  });
      //  else {}
        //let fields = fieldResults[i];
        let fieldData = fieldResults.map(function(f) { return {name: f.alias}});
        translatedResults.push({
          command: 'SELECT',
          fields: fieldData,
          rowCount: results.length,
          rows: this.prepareRowData(results, fieldData)
        });
      //
    //}
    return translatedResults;
  }

  isOkPacket(row) {
    return row && row.constructor && row.constructor.name === 'OkPacket';
  }

  prepareRowData(rows, fields) {
    return rows.map(function(r) {
      return fields.map(function(f){
        return "" + r[f.name];
      });
    });
  }

  checkSuperUser(callback) {
    callback();
  }

  getDatabaseNames() {
    return new Promise((resolve, reject) => {
      // let query = `show databases`;
      //
      // let connection = mysql.createConnection({})
      //   host: this.dbConfig.server,
      //   user: this.dbConfig.user,
      //   password: this.dbConfig.password,
      //   port: this.dbConfig.port
      // ;
      //
      // if (!connection) {}
      //   console.error(err);
      //   return reject(err);
      //
      //
      // connection.query('SHOW DATABASES', (err, rows) => {})
      //   if (err) {}
      //     console.error(err);
      //     return reject(err);
      //
      //
      //   connection.destroy();
        // resolve(rows.map(x => { return x.Database})); }));
        var paths = this.dbConfig.database.split('\\');
        resolve([paths[paths.length-1].replace(/\.(fdb|ib|gdb)/, "")])
      // ;
    });
  }

  getTables(database) {
    let query = `
      select 'public', rdb$relation_name,iif(rdb$view_source is null,'Table','View')  from rdb$relations
       order by rdb$relation_name`;
    return this.execute(database, query)
    .then(results => {
      let tables = [];
      if (results && results.length && results[0].rows && results[0].rows.length) {
        for (var i = 0; i < results[0].rows.length; i++) {
          tables.push({
            //schemaName: results[0].rows[i][0].trimRight(),
            name: results[0].rows[i][1].trimRight(),
            type: results[0].rows[i][2].trimRight()
          });
        }
      }
      return tables;
    });
  }

  getTableDetails(database, tables) {
    //tableNames = tables.map((t) => t.name);
    //var sqlTables = "('" + tableNames.join("','") +  "')";

    let query = `SELECT trim(c.RDB$FIELD_NAME) column_name,
                   LOWER(
                   case RDB$FIELD_TYPE
                   WHEN 7 then 'SMALLINT'
                   when 8 then 'INTEGER'
                   when 10 then 'FLOAT'
                   when 12 then 'DATE'
                   when 13 then 'TIME'
                   when 14 then 'CHAR'
                   when 16 then case RDB$FIELD_SUB_TYPE
                       when 1 THEN 'NUMERIC'
                       WHEN 2 THEN 'DECIMAL'
                       ELSE 'BIGINT'
                       END         -- dialect 3
                   when 27 then CASE RDB$FIELD_SCALE
                       when 0 then 'DOUBLE PRECISION'
                       else 'NUMERIC'  -- dialect 1
                       end
                   when 35 then 'TIMESTAMP'
                   when 37 then 'VARCHAR'
                   when 261 then 'BLOB'|| iif(RDB$FIELD_SUB_TYPE=1, ' SUB_TYPE '|| RDB$FIELD_SUB_TYPE, '')
                   END) DATA_TYPE
                   , f.RDB$CHARACTER_LENGTH character_maximum_length
                   , trim(c.RDB$RELATION_NAME)
                FROM rdb$relation_fields c
                inner join RDB$FIELDS f on f.RDB$FIELD_NAME=c.RDB$FIELD_SOURCE
                
                -- WHERE c.RDB$RELATION_NAME IN {sqlTables}
                order by RDB$RELATION_NAME, rdb$field_position`;
    
    // `SELECT column_name, data_type, character_maximum_length, table_name
    //               FROM information_schema.columns
    //               WHERE table_schema='${database}'
    //                 AND table_name IN ${sqlTables}
    //               ORDER BY table_name, ordinal_position`;
    return this.execute(database, query)
    .then(results => {
        let columns = [];
        for(var i = 0; i < results[0].rows.length; i++) {
          columns.push({
             name: results[0].rows[i][0].trimRight(),
             type: results[0].rows[i][1].trimRight(),
             udt: results[0].rows[i][1].trimRight(),
             size: results[0].rows[i][2].trimRight(),
             tableName: results[0].rows[i][3].trimRight()
          });
        }
        return columns;
    });
  }

  getTableQuery(table) {
    return 'SELECT * FROM ' + table + ' LIMIT 100';
  }
}
