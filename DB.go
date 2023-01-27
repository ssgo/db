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
	pwd           string
	Host          string
	ReadonlyHosts []string
	DB            string
	SSL           string
	MaxOpens      int
	MaxIdles      int
	MaxLifeTime   int
	LogSlow       config.Duration
	logger        *log.Logger
}

type dbSSL struct {
	Ca       string
	Cert     string
	Key      string
	Insecure bool
}

func (dbInfo *dbInfo) Dsn() string {
	if strings.HasPrefix(dbInfo.Type, "sqlite") {
		return fmt.Sprintf("%s://%s:****@%s?logSlow=%s", dbInfo.Type, dbInfo.User, dbInfo.Host, dbInfo.LogSlow.TimeDuration())
	} else {
		//if []byte(conf.Host)[0] == '/' {
		//	return conf.Host
		//}
		hosts := []string{dbInfo.Host}
		if dbInfo.ReadonlyHosts != nil {
			hosts = append(hosts, dbInfo.ReadonlyHosts...)
		}
		sslStr := ""
		if dbInfo.SSL != "" {
			sslStr = "&tls=" + dbInfo.SSL
		}
		return fmt.Sprintf("%s://%s:****@%s/%s?logSlow=%s"+sslStr, dbInfo.Type, dbInfo.User, strings.Join(hosts, ","), dbInfo.DB, dbInfo.LogSlow.TimeDuration())
	}
}

func (dbInfo *dbInfo) ConfigureBy(setting string) {
	urlInfo, err := url.Parse(setting)
	if err != nil {
		dbInfo.logger.Error(err.Error(), "url", setting)
		return
	}

	dbInfo.Type = urlInfo.Scheme
	if strings.HasPrefix(dbInfo.Type, "sqlite") {
		dbInfo.Host = urlInfo.Host + urlInfo.Path
	} else {
		if strings.ContainsRune(urlInfo.Host, ',') {
			// 多个节点，读写分离
			a := strings.Split(urlInfo.Host, ",")
			dbInfo.Host = a[0]
			dbInfo.ReadonlyHosts = a[1:]
		} else {
			dbInfo.Host = urlInfo.Host
			dbInfo.ReadonlyHosts = nil
		}
		if len(urlInfo.Path) > 1 {
			dbInfo.DB = urlInfo.Path[1:]
		}
	}
	dbInfo.User = urlInfo.User.Username()
	dbInfo.pwd, _ = urlInfo.User.Password()
	dbInfo.Password = ""

	dbInfo.MaxIdles = u.Int(urlInfo.Query().Get("maxIdles"))
	dbInfo.MaxLifeTime = u.Int(urlInfo.Query().Get("maxLifeTime"))
	dbInfo.MaxOpens = u.Int(urlInfo.Query().Get("maxOpens"))
	dbInfo.LogSlow = config.Duration(u.Duration(urlInfo.Query().Get("logSlow")))
	dbInfo.SSL = urlInfo.Query().Get("tls")
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
var dbConfigsLock = sync.RWMutex{}
var dbSSLs = make(map[string]*dbSSL)
var dbInstances = make(map[string]*DB)
var dbInstancesLock = sync.RWMutex{}
var once sync.Once

func GetDB(name string, logger *log.Logger) *DB {
	if logger == nil {
		logger = log.DefaultLogger
	}

	dbInstancesLock.RLock()
	oldConn := dbInstances[name]
	dbInstancesLock.RUnlock()
	if oldConn != nil {
		return oldConn.CopyByLogger(logger)
	}

	var conf *dbInfo
	if strings.Contains(name, "://") {
		conf = new(dbInfo)
		conf.logger = logger
		conf.ConfigureBy(name)
	} else {
		dbConfigsLock.RLock()
		n := len(dbConfigs)
		dbConfigsLock.RUnlock()
		if n == 0 {
			once.Do(func() {
				errs := config.LoadConfig("db", &dbConfigs)
				if errs != nil {
					for _, err := range errs {
						logger.Error(err.Error())
					}
				}
			})
		}
		dbConfigsLock.RLock()
		conf = dbConfigs[name]
		dbConfigsLock.RUnlock()
		if conf == nil {
			conf = new(dbInfo)
			dbConfigsLock.Lock()
			dbConfigs[name] = conf
			dbConfigsLock.Unlock()
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

	if conf.SSL != "" && len(dbSSLs) == 0 {
		config.LoadConfig("dbssl", &dbSSLs)
		for sslName, sslInfo := range dbSSLs {
			decryptedCa := u.DecryptAes(sslInfo.Ca, settedKey, settedIv)
			decryptedCert := u.DecryptAes(sslInfo.Cert, settedKey, settedIv)
			decryptedKey := u.DecryptAes(sslInfo.Key, settedKey, settedIv)
			if decryptedCa == "" {
				decryptedCa = sslInfo.Ca
			}
			if decryptedCert == "" {
				decryptedCert = sslInfo.Cert
			}
			if decryptedKey == "" {
				decryptedKey = sslInfo.Key
			}
			RegisterSSL(sslName, decryptedCa, decryptedCert, decryptedKey, sslInfo.Insecure)
		}
	}

	if conf.SSL != "" && dbSSLs[conf.SSL] == nil {
		logger.Error("dbssl config lost")
	}

	if strings.ContainsRune(conf.Host, ',') {
		// 多个节点，读写分离
		a := strings.Split(conf.Host, ",")
		conf.Host = a[0]
		conf.ReadonlyHosts = a[1:]
	} else {
		conf.ReadonlyHosts = nil
	}

	if conf.Password != "" {
		conf.pwd = u.DecryptAes(conf.Password, settedKey, settedIv)
		if conf.pwd == "" {
			log.DefaultLogger.Warning("password is invalid")
			conf.pwd = conf.Password
		}
	} else {
		if !strings.HasPrefix(conf.Type, "sqlite") {
			//logWarn("password is empty", nil)
			logger.Warning("password is empty")
		}
	}
	conf.Password = ""

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
	//conn, err := getPool(conf.Type, conf.Host, conf.User, decryptedPassword, conf.DB)
	conn, err := getPool(conf)
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
			conn, err := getPoolForHost(conf, host)
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
	dbInstancesLock.Lock()
	dbInstances[name] = db
	dbInstancesLock.Unlock()
	return db.CopyByLogger(logger)
}

//func getPool(typ, host, user, pwd, db string) (*sql.DB, error) {
func getPool(conf *dbInfo) (*sql.DB, error) {
	return getPoolForHost(conf, "")
}

func getPoolForHost(conf *dbInfo, host string) (*sql.DB, error) {
	connectType := "tcp"
	if host == "" {
		host = conf.Host
	}
	if []byte(host)[0] == '/' {
		connectType = "unix"
	}

	dsn := ""
	if strings.HasPrefix(conf.Type, "sqlite") {
		dsn = host
		if conf.pwd != "" {
			dsn += fmt.Sprint("?_auth&_auth_user=", conf.User, "&_auth_pass=", conf.pwd, "&_auth_crypt=sha512")
		}
	} else {
		sslStr := ""
		if conf.SSL != "" {
			sslStr = "?tls=" + conf.SSL
		}
		dsn = fmt.Sprintf("%s:%s@%s(%s)/%s"+sslStr, conf.User, conf.pwd, connectType, host, conf.DB)
	}
	//fmt.Println(222, dsn)
	return sql.Open(conf.Type, dsn)
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
