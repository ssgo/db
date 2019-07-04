package db

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ssgo/log"
	"reflect"
	"strings"
	"time"
)

func basePrepare(db *sql.DB, tx *sql.Tx, requestSql string) *Stmt {
	var sqlStmt *sql.Stmt
	var err error
	if tx != nil {
		sqlStmt, err = tx.Prepare(requestSql)
	} else if db != nil {
		sqlStmt, err = db.Prepare(requestSql)
	} else {
		return &Stmt{Error: errors.New("operate on a bad connection")}
	}
	if err != nil {
		return &Stmt{Error: err}
	}
	return &Stmt{conn: sqlStmt, lastSql: &requestSql}
}

func baseExec(db *sql.DB, tx *sql.Tx, requestSql string, args ...interface{}) *ExecResult {
	var r sql.Result
	var err error
	startTime := time.Now()
	if tx != nil {
		r, err = tx.Exec(requestSql, args...)
	} else if db != nil {
		r, err = db.Exec(requestSql, args...)
	} else {
		return &ExecResult{Sql: &requestSql, Args: args, usedTime: log.MakeUesdTime(startTime, time.Now()), Error: errors.New("operate on a bad connection")}
	}
	endTime := time.Now()

	if err != nil {
		return &ExecResult{Sql: &requestSql, Args: args, usedTime: log.MakeUesdTime(startTime, endTime), Error: err}
	}
	return &ExecResult{Sql: &requestSql, Args: args, usedTime: log.MakeUesdTime(startTime, endTime), result: r}
}

func baseQuery(db *sql.DB, tx *sql.Tx, requestSql string, args ...interface{}) *QueryResult {
	var rows *sql.Rows
	var err error
	startTime := time.Now()
	if tx != nil {
		rows, err = tx.Query(requestSql, args...)
	} else if db != nil {
		rows, err = db.Query(requestSql, args...)
	} else {
		return &QueryResult{Sql: &requestSql, Args: args, usedTime: log.MakeUesdTime(startTime, time.Now()), Error: errors.New("operate on a bad connection")}
	}
	endTime := time.Now()

	if err != nil {
		return &QueryResult{Sql: &requestSql, Args: args, usedTime: log.MakeUesdTime(startTime, endTime), Error: err}
	}
	return &QueryResult{Sql: &requestSql, Args: args, usedTime: log.MakeUesdTime(startTime, endTime), rows: rows}
}

func makeInsertSql(table string, data interface{}, useReplace bool) (string, []interface{}) {
	keys, vars, values := makeKeysVarsValues(data)
	var operation string
	if useReplace {
		operation = "replace"
	} else {
		operation = "insert"
	}
	requestSql := fmt.Sprintf("%s into `%s` (`%s`) values (%s)", operation, table, strings.Join(keys, "`,`"), strings.Join(vars, ","))
	return requestSql, values
}

func makeUpdateSql(table string, data interface{}, wheres string, args ...interface{}) (string, []interface{}) {
	keys, vars, values := makeKeysVarsValues(data)
	for i, k := range keys {
		keys[i] = fmt.Sprintf("`%s`=%s", k, vars[i])
	}
	for _, v := range args {
		values = append(values, v)
	}
	requestSql := fmt.Sprintf("update `%s` set %s where %s", table, strings.Join(keys, ","), wheres)
	return requestSql, values
}

func makeKeysVarsValues(data interface{}) ([]string, []string, []interface{}) {
	keys := make([]string, 0)
	vars := make([]string, 0)
	values := make([]interface{}, 0)

	dataType := reflect.TypeOf(data)
	dataValue := reflect.ValueOf(data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
		dataValue = dataValue.Elem()
	}

	if dataType.Kind() == reflect.Struct {
		// 按结构处理数据
		for i := 0; i < dataType.NumField(); i++ {
			v := dataValue.Field(i)
			if v.Kind() == reflect.Interface {
				v = v.Elem()
			}
			keys = append(keys, dataType.Field(i).Name)


			if v.Kind() == reflect.String && len(v.String()) > 0 && []byte(v.String())[0] == ':' {
				vars = append(vars, string([]byte(v.String())[1:]))
			} else {
				vars = append(vars, "?")
				values = append(values, v.Interface())
			}
		}
	} else if dataType.Kind() == reflect.Map {
		// 按Map处理数据
		for _, k := range dataValue.MapKeys() {
			v := dataValue.MapIndex(k)
			if v.Kind() == reflect.Interface {
				v = v.Elem()
			}
			keys = append(keys, k.String())
			if v.Kind() == reflect.String && v.Len() > 0 && []byte(v.String())[0] == ':' {
				vars = append(vars, string([]byte(v.String())[1:]))
			} else {
				vars = append(vars, "?")
				values = append(values, v.Interface())
			}
		}
	}

	return keys, vars, values
}

//func logError(err error, info *string, args []interface{}) {
//	if enabledLogs && err != nil {
//		log.Error("DB", map[string]interface{}{
//			"sql":   *info,
//			"args":  args,
//			"error": err.Error(),
//		}, "/ssgo/db/")
//	}
//}

//func logWarn(info string, args []interface{}) {
//	if enabledLogs {
//		log.Warning("DB", map[string]interface{}{
//			"warn": info,
//			"args": args,
//		})
//	}
//}
