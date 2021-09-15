package db

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ssgo/config"
	"github.com/ssgo/log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ssgo/u"
)

type dbInfo struct {
	Type          string
	User          string
	Password      string
	Host          string
	ReadonlyHosts []string
	DB            string
	MaxOpens      int
	MaxIdles      int
	MaxLifeTime   int
	LogSlow       config.Duration
	logger        *log.Logger
}

func (conf *dbInfo) Dsn() string {
	if strings.HasPrefix(conf.Type, "sqlite") {
		return fmt.Sprintf("%s://%s:****@%s?logSlow=%s", conf.Type, conf.User, conf.Host, conf.LogSlow.TimeDuration())
	} else {
		//if []byte(conf.Host)[0] == '/' {
		//	return conf.Host
		//}
		hosts := []string{conf.Host}
		if conf.ReadonlyHosts != nil {
			hosts = append(hosts, conf.ReadonlyHosts...)
		}
		return fmt.Sprintf("%s://%s:****@%s/%s?logSlow=%s", conf.Type, conf.User, strings.Join(hosts, ","), conf.DB, conf.LogSlow.TimeDuration())
	}
}

func (conf *dbInfo) ConfigureBy(setting string) {
	urlInfo, err := url.Parse(setting)
	if err != nil {
		conf.logger.Error(err.Error(), "url", setting)
		return
	}

	conf.Type = urlInfo.Scheme
	if strings.HasPrefix(conf.Type, "sqlite") {
		conf.Host = urlInfo.Host + urlInfo.Path
	} else {
		if strings.ContainsRune(urlInfo.Host, ',') {
			// 多个节点，读写分离
			a := strings.Split(urlInfo.Host, ",")
			conf.Host = a[0]
			conf.ReadonlyHosts = a[1:]
		} else {
			conf.Host = urlInfo.Host
			conf.ReadonlyHosts = nil
		}
		if len(urlInfo.Path) > 1 {
			conf.DB = urlInfo.Path[1:]
		}
	}
	conf.User = urlInfo.User.Username()
	pwd, _ := urlInfo.User.Password()
	conf.Password = pwd

	conf.MaxIdles = u.Int(urlInfo.Query().Get("maxIdles"))
	conf.MaxLifeTime = u.Int(urlInfo.Query().Get("maxLifeTime"))
	conf.MaxOpens = u.Int(urlInfo.Query().Get("maxOpens"))
	conf.LogSlow = config.Duration(u.Duration(urlInfo.Query().Get("logSlow")))
}

type DB struct {
	conn                *sql.DB
	readonlyConnections []*sql.DB
	Config              *dbInfo
	logger              *dbLogger
	Error               error
}

// var settedKey = []byte("vpL54DlR2KG{JSAaAX7Tu;*#&DnG`M0o")
// var settedIv = []byte("@z]zv@10-K.5Al0Dm`@foq9k\"VRfJ^~j")
var settedKey = []byte("?GQ$0K0GgLdO=f+~L68PLm$uhKr4'=tV")
var settedIv = []byte("VFs7@sK61cj^f?HZ")
var keysSetted = false

func SetEncryptKeys(key, iv []byte) {
	if !keysSetted {
		settedKey = key
		settedIv = iv
		keysSetted = true
	}
}

type dbLogger struct {
	config *dbInfo
	logger *log.Logger
}

func (dl *dbLogger) LogError(error string) {
	dl.logger.DBError(error, dl.config.Type, dl.config.Dsn(), "", nil, 0)
}

func (dl *dbLogger) LogQuery(query string, args []interface{}, usedTime float32) {
	dl.logger.DB(dl.config.Type, dl.config.Dsn(), query, args, usedTime)
}

func (dl *dbLogger) LogQueryError(error string, query string, args []interface{}, usedTime float32) {
	dl.logger.DBError(error, dl.config.Type, dl.config.Dsn(), query, args, usedTime)
}

var dbConfigs = make(map[string]*dbInfo)
var dbInstances = make(map[string]*DB)
var once sync.Once

func GetDB(name string, logger *log.Logger) *DB {
	if logger == nil {
		logger = log.DefaultLogger
	}

	if dbInstances[name] != nil {
		return dbInstances[name].CopyByLogger(logger)
	}

	var conf *dbInfo
	if strings.Contains(name, "://") {
		conf = new(dbInfo)
		conf.logger = logger
		conf.ConfigureBy(name)
	} else {
		if len(dbConfigs) == 0 {
			once.Do(func() {
				errs := config.LoadConfig("db", &dbConfigs)
				if errs != nil {
					for _, err := range errs {
						logger.Error(err.Error())
					}
				}
			})
		}
		conf = dbConfigs[name]
		if conf == nil {
			conf = new(dbInfo)
			dbConfigs[name] = conf
		}
	}
	if conf.Host == "" {
		conf.Host = "127.0.0.1:3306"
	}
	if conf.Type == "" {
		conf.Type = "mysql"
	}
	if conf.User == "" {
		conf.User = "root"
	}
	if conf.DB == "" {
		conf.DB = "test"
	}

	if strings.ContainsRune(conf.Host, ',') {
		// 多个节点，读写分离
		a := strings.Split(conf.Host, ",")
		conf.Host = a[0]
		conf.ReadonlyHosts = a[1:]
	} else {
		conf.ReadonlyHosts = nil
	}

	decryptedPassword := ""
	if conf.Password != "" {
		decryptedPassword = u.DecryptAes(conf.Password, settedKey, settedIv)
		if decryptedPassword == "" {
			log.DefaultLogger.Warning("password is invalid")
			decryptedPassword = conf.Password
		}
	} else {
		if !strings.HasPrefix(conf.Type, "sqlite") {
			//logWarn("password is empty", nil)
			logger.Warning("password is empty")
		}
	}
	conf.Password = u.UniqueId()

	//connectType := "tcp"
	//if []byte(conf.Host)[0] == '/' {
	//	connectType = "unix"
	//}
	//
	//dsn := ""
	//if isSqlite {
	//	dsn = conf.Host
	//} else {
	//	dsn = fmt.Sprintf("%s:%s@%s(%s)/%s", conf.User, decryptedPassword, connectType, conf.Host, conf.DB)
	//}
	//conn, err := sql.Open(conf.Type, dsn)
	//if err != nil {
	//	logger.DBError(err.Error(), conf.Type, conf.Dsn(), "", nil, 0)
	//	return &DB{conn: nil, Error: err}
	//}
	conn, err := getPool(conf.Type, conf.Host, conf.User, decryptedPassword, conf.DB)
	if err != nil {
		logger.DBError(err.Error(), conf.Type, conf.Dsn(), "", nil, 0)
		return &DB{conn: nil, Error: err}
	}

	db := new(DB)
	db.conn = conn

	// 创建只读连接池
	if conf.ReadonlyHosts != nil {
		readonlyConnections := make([]*sql.DB, 0)
		for _, host := range conf.ReadonlyHosts {
			conn, err := getPool(conf.Type, host, conf.User, decryptedPassword, conf.DB)
			if err != nil {
				logger.DBError(err.Error(), conf.Type, conf.Dsn(), "", nil, 0)
			} else {
				readonlyConnections = append(readonlyConnections, conn)
			}
		}
		if len(readonlyConnections) > 0 {
			db.readonlyConnections = readonlyConnections
		}
	}

	db.Error = nil
	db.Config = conf
	if conf.MaxIdles > 0 {
		conn.SetMaxIdleConns(conf.MaxIdles)
	}
	if conf.MaxOpens > 0 {
		conn.SetMaxOpenConns(conf.MaxOpens)
	}
	if conf.MaxLifeTime > 0 {
		conn.SetConnMaxLifetime(time.Second * time.Duration(conf.MaxLifeTime))
	}
	if conf.LogSlow == 0 {
		conf.LogSlow = config.Duration(1000 * time.Millisecond)
	}
	dbInstances[name] = db
	return db.CopyByLogger(logger)
}

func getPool(typ, host, user, pwd, db string) (*sql.DB, error) {
	connectType := "tcp"
	if []byte(host)[0] == '/' {
		connectType = "unix"
	}

	dsn := ""
	if strings.HasPrefix(typ, "sqlite") {
		dsn = host
	} else {
		dsn = fmt.Sprintf("%s:%s@%s(%s)/%s", user, pwd, connectType, host, db)
	}
	return sql.Open(typ, dsn)
}

func (db *DB) CopyByLogger(logger *log.Logger) *DB {
	newDB := new(DB)
	newDB.conn = db.conn
	newDB.readonlyConnections = db.readonlyConnections
	newDB.Config = db.Config
	if logger == nil {
		logger = log.DefaultLogger
	}
	newDB.logger = &dbLogger{logger: logger, config: db.Config}
	return newDB
}

func (db *DB) SetLogger(logger *log.Logger) {
	db.logger.logger = logger
}

func (db *DB) GetLogger() *log.Logger {
	return db.logger.logger
}

func (db *DB) Destroy() error {
	if db.conn == nil {
		return errors.New("operate on a bad connection")
	}
	err := db.conn.Close()
	//logError(err, nil, nil)
	if err != nil {
		db.logger.LogError(err.Error())
	}
	return err
}

func (db *DB) GetOriginDB() *sql.DB {
	if db.conn == nil {
		return nil
	}
	return db.conn
}

func (db *DB) Prepare(requestSql string) *Stmt {
	stmt := basePrepare(db.conn, nil, requestSql)
	stmt.logger = db.logger
	if stmt.Error != nil {
		db.logger.LogError(stmt.Error.Error())
	}
	return stmt
}

func (db *DB) Begin() *Tx {
	if db.conn == nil {
		return &Tx{logSlow: db.Config.LogSlow.TimeDuration(), Error: errors.New("operate on a bad connection"), logger: db.logger}
	}
	sqlTx, err := db.conn.Begin()
	if err != nil {
		db.logger.LogError(err.Error())
		return &Tx{logSlow: db.Config.LogSlow.TimeDuration(), Error: nil, logger: db.logger}
	}
	return &Tx{logSlow: db.Config.LogSlow.TimeDuration(), conn: sqlTx, logger: db.logger}
}

func (db *DB) Exec(requestSql string, args ...interface{}) *ExecResult {
	r := baseExec(db.conn, nil, requestSql, args...)
	r.logger = db.logger
	if r.Error != nil {
		db.logger.LogQueryError(r.Error.Error(), requestSql, args, r.usedTime)
	} else {
		if db.Config.LogSlow > 0 && r.usedTime >= float32(db.Config.LogSlow.TimeDuration()/time.Millisecond) {
			// 记录慢请求日志
			db.logger.LogQuery(requestSql, args, r.usedTime)
		}
	}
	return r
}

func (db *DB) Query(requestSql string, args ...interface{}) *QueryResult {
	conn := db.conn
	if db.readonlyConnections != nil {
		connNum := len(db.readonlyConnections)
		if connNum == 1 {
			conn = db.readonlyConnections[0]
		} else {
			p := u.GlobalRand1.Intn(connNum)
			conn = db.readonlyConnections[p]
		}
	}

	r := baseQuery(conn, nil, requestSql, args...)
	r.logger = db.logger
	if r.Error != nil {
		db.logger.LogQueryError(r.Error.Error(), requestSql, args, r.usedTime)
	} else {
		if db.Config.LogSlow > 0 && r.usedTime >= float32(db.Config.LogSlow.TimeDuration()/time.Millisecond) {
			// 记录慢请求日志
			db.logger.LogQuery(requestSql, args, r.usedTime)
		}
	}
	return r
}

func (db *DB) Insert(table string, data interface{}) *ExecResult {
	requestSql, values := makeInsertSql(table, data, false)
	r := baseExec(db.conn, nil, requestSql, values...)
	r.logger = db.logger
	if r.Error != nil {
		db.logger.LogQueryError(r.Error.Error(), requestSql, values, r.usedTime)
	} else {
		if db.Config.LogSlow > 0 && r.usedTime >= float32(db.Config.LogSlow.TimeDuration()/time.Millisecond) {
			// 记录慢请求日志
			db.logger.LogQuery(requestSql, values, r.usedTime)
		}
	}
	return r
}
func (db *DB) Replace(table string, data interface{}) *ExecResult {
	requestSql, values := makeInsertSql(table, data, true)
	r := baseExec(db.conn, nil, requestSql, values...)
	r.logger = db.logger
	if r.Error != nil {
		db.logger.LogQueryError(r.Error.Error(), requestSql, values, r.usedTime)
	} else {
		if db.Config.LogSlow > 0 && r.usedTime >= float32(db.Config.LogSlow.TimeDuration()/time.Millisecond) {
			// 记录慢请求日志
			db.logger.LogQuery(requestSql, values, r.usedTime)
		}
	}
	return r
}

func (db *DB) Update(table string, data interface{}, wheres string, args ...interface{}) *ExecResult {
	requestSql, values := makeUpdateSql(table, data, wheres, args...)
	r := baseExec(db.conn, nil, requestSql, values...)
	r.logger = db.logger
	if r.Error != nil {
		db.logger.LogQueryError(r.Error.Error(), requestSql, values, r.usedTime)
	} else {
		if db.Config.LogSlow > 0 && r.usedTime >= float32(db.Config.LogSlow.TimeDuration()/time.Millisecond) {
			// 记录慢请求日志
			db.logger.LogQuery(requestSql, values, r.usedTime)
		}
	}
	return r
}

func (db *DB) InKeys(numArgs int) string {
	return InKeys(numArgs)
}

func InKeys(numArgs int) string {
	a := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		a[i] = "?"
	}
	return fmt.Sprintf("(%s)", strings.Join(a, ","))
}
