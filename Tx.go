package db

import (
	"database/sql"
	"errors"
)

type Tx struct {
	conn     *sql.Tx
	lastSql  *string
	lastArgs []interface{}
	Error    error
	logger   *dbLogger
	logSlow  int
}

func (tx *Tx) Commit() error {
	if tx.conn == nil {
		return errors.New("operate on a bad connection")
	}
	err := tx.conn.Commit()
	if err != nil {
		tx.logger.LogQueryError(err.Error(), *tx.lastSql, tx.lastArgs, -1)
	}
	return err
}

func (tx *Tx) Rollback() error {
	if tx.conn == nil {
		return errors.New("operate on a bad connection")
	}
	err := tx.conn.Rollback()
	//logError(err.Error(), *tx.lastSql, tx.lastArgs)
	if err != nil {
		tx.logger.LogQueryError(err.Error(), *tx.lastSql, tx.lastArgs, -1)
	}
	return err
}

func (tx *Tx) Prepare(requestSql string) *Stmt {
	r := basePrepare(nil, tx.conn, requestSql)
	r.logger = tx.logger
	if r.Error != nil {
		tx.logger.LogQueryError(r.Error.Error(), *tx.lastSql, tx.lastArgs, -1)
	}
	return r
}

func (tx *Tx) Exec(requestSql string, args ...interface{}) *ExecResult {
	tx.lastSql = &requestSql
	tx.lastArgs = args
	r := baseExec(nil, tx.conn, requestSql, args...)
	r.logger = tx.logger
	if r.Error != nil {
		tx.logger.LogQueryError(r.Error.Error(), *tx.lastSql, tx.lastArgs, r.usedTime)
	} else {
		if tx.logSlow == -1 || r.usedTime >= float32(tx.logSlow) {
			// 记录慢请求日志
			tx.logger.LogQuery(*tx.lastSql, tx.lastArgs, r.usedTime)
		}
	}
	return r
}

func (tx *Tx) Query(requestSql string, args ...interface{}) *QueryResult {
	tx.lastSql = &requestSql
	tx.lastArgs = args
	r := baseQuery(nil, tx.conn, requestSql, args...)
	r.logger = tx.logger
	if r.Error != nil {
		tx.logger.LogQueryError(r.Error.Error(), *tx.lastSql, tx.lastArgs, r.usedTime)
	} else {
		if tx.logSlow == -1 || r.usedTime >= float32(tx.logSlow) {
			// 记录慢请求日志
			tx.logger.LogQuery(*tx.lastSql, tx.lastArgs, r.usedTime)
		}
	}
	return r
}

func (tx *Tx) Insert(table string, data interface{}) *ExecResult {
	requestSql, values := makeInsertSql(table, data, false)
	tx.lastSql = &requestSql
	tx.lastArgs = values
	r := baseExec(nil, tx.conn, requestSql, values...)
	r.logger = tx.logger
	if r.Error != nil {
		tx.logger.LogQueryError(r.Error.Error(), *tx.lastSql, tx.lastArgs, r.usedTime)
	} else {
		if tx.logSlow == -1 || r.usedTime >= float32(tx.logSlow) {
			// 记录慢请求日志
			tx.logger.LogQuery(*tx.lastSql, tx.lastArgs, r.usedTime)
		}
	}
	return r
}
func (tx *Tx) Replace(table string, data interface{}) *ExecResult {
	requestSql, values := makeInsertSql(table, data, true)
	tx.lastSql = &requestSql
	tx.lastArgs = values
	r := baseExec(nil, tx.conn, requestSql, values...)
	r.logger = tx.logger
	if r.Error != nil {
		tx.logger.LogQueryError(r.Error.Error(), *tx.lastSql, tx.lastArgs, r.usedTime)
	} else {
		if tx.logSlow == -1 || r.usedTime >= float32(tx.logSlow) {
			// 记录慢请求日志
			tx.logger.LogQuery(*tx.lastSql, tx.lastArgs, r.usedTime)
		}
	}
	return r
}

func (tx *Tx) Update(table string, data interface{}, wheres string, args ...interface{}) *ExecResult {
	requestSql, values := makeUpdateSql(table, data, wheres, args...)
	tx.lastSql = &requestSql
	tx.lastArgs = values
	r := baseExec(nil, tx.conn, requestSql, values...)
	r.logger = tx.logger
	if r.Error != nil {
		tx.logger.LogQueryError(r.Error.Error(), *tx.lastSql, tx.lastArgs, r.usedTime)
	} else {
		if tx.logSlow == -1 || r.usedTime >= float32(tx.logSlow) {
			// 记录慢请求日志
			tx.logger.LogQuery(*tx.lastSql, tx.lastArgs, r.usedTime)
		}
	}
	return r
}
