package db

import (
	"database/sql"
	"errors"
	"github.com/ssgo/log"
	"time"
)

type Stmt struct {
	conn     *sql.Stmt
	lastSql  *string
	lastArgs []interface{}
	Error    error
	logger *dbLogger
}

func (stmt *Stmt) Exec(args ...interface{}) *ExecResult {
	stmt.lastArgs = args
	if stmt.conn == nil {
		return &ExecResult{Sql: stmt.lastSql, Args: stmt.lastArgs, usedTime: -1, logger: stmt.logger, Error: errors.New("operate on a bad connection")}
	}
	startTime := time.Now()
	r, err := stmt.conn.Exec(args...)
	endTime := time.Now()
	if err != nil {
		//logError(err, stmt.lastSql, stmt.lastArgs)
		stmt.logger.LogQueryError(err.Error(), *stmt.lastSql, stmt.lastArgs, log.MakeUesdTime(startTime, endTime))
		return &ExecResult{Sql: stmt.lastSql, Args: stmt.lastArgs, usedTime: log.MakeUesdTime(startTime, endTime), logger: stmt.logger, Error: err}
	}
	return &ExecResult{Sql: stmt.lastSql, Args: stmt.lastArgs, usedTime: log.MakeUesdTime(startTime, endTime), logger: stmt.logger, result: r}
}

func (stmt *Stmt) Close() error {
	if stmt.conn == nil {
		return errors.New("operate on a bad connection")
	}
	err := stmt.conn.Close()
	if err != nil {
		stmt.logger.LogQueryError(err.Error(), *stmt.lastSql, stmt.lastArgs, -1)
	}
	return err
}
