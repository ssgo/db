package db

import (
	"github.com/ssgo/log"
	"github.com/ssgo/u"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//var dbset = "mysql://root:@localhost/test?logSlow=1"
//var dbset = "mysql://localhost/test"
var dbset = "test"

type userInfo struct {
	innerId int
	Tag     string
	Id      int
	Name    string
	Phone   string
	Email   string
	Parents []string
	Active  bool
	Time    string
}

type UserBaseModel struct {
	Id       int
	Name     string
	Password string
}

type UserModel struct {
	UserBaseModel
	Phone      string
	Active     bool
	Parents    []string
	UserStatus int
	Owner      int
	Salt       string
}

func TestMakeInsertSql(t *testing.T) {
	user := &UserModel{
		UserBaseModel: UserBaseModel{
			Name:     "王二小",
			Password: "2121asds",
		},
		Parents:    []string{"aa", "bb"},
		Active:     true,
		UserStatus: 1, //正常,
		Salt:       "de312",
	}

	requestSql, _ := makeInsertSql("table_name", user, false)
	if requestSql != "insert into `table_name` (`Id`,`Name`,`Password`,`Phone`,`Active`,`Parents`,`UserStatus`,`Owner`,`Salt`) values (?,?,?,?,?,?,?,?,?)" {
		t.Fatal("MakeInsertSql requestSql error ", requestSql)
	}
}

func TestBaseSelect(t *testing.T) {

	sql := "SELECT 1002 id, '13800000001' phone"
	db := GetDB("test2", nil)
	if db.Error != nil {
		t.Fatal("GetDB error", db.Error)
		return
	}

	//results1 := make([]map[string]interface{}, 0)
	r := db.Query(sql)
	if r.Error != nil {
		t.Fatal("Query error", sql, r)
	}
	results1 := r.MapResults()
	if results1[0]["id"].(int64) != 1002 || results1[0]["phone"].(string) != "13800000001" {
		t.Fatal("Result error", sql, results1, r)
	}

	//results2 := make([]map[string]string, 0)
	r = db.Query(sql)
	if r.Error != nil {
		t.Fatal("Query error", sql, r)
	}
	results2 := r.StringMapResults()
	if results2[0]["id"] != "1002" || results2[0]["phone"] != "13800000001" {
		t.Fatal("Result error", sql, results2, r)
	}

	results3 := make([]map[string]int, 0)
	r = db.Query(sql)
	if r.Error != nil {
		t.Fatal("Query error", sql, results3, r)
	}
	r.To(&results3)
	if results3[0]["id"] != 1002 || results3[0]["phone"] != 13800000001 {
		t.Fatal("Result error", sql, results3, r)
	}

	results4 := make([]userInfo, 0)
	r = db.Query(sql)
	if r.Error != nil {
		t.Fatal("Query error", sql, results4, r)
	}
	r.To(&results4)
	if results4[0].Id != 1002 || results4[0].Phone != "13800000001" {
		t.Fatal("Result error", sql, results4, r)
	}

	//results5 := make([][]string, 0)
	results5 := db.Query(sql).StringSliceResults()
	if results5[0][0] != "1002" || results5[0][1] != "13800000001" {
		t.Fatal("Result error", sql, results5, r)
	}

	//results6 := make([]string, 0)
	r = db.Query(sql)
	if r.Error != nil {
		t.Fatal("Query error", sql, r)
	}
	results6 := r.StringsOnC1()
	if results6[0] != "1002" {
		t.Fatal("Result error", sql, results6, r)
	}

	//results7 := map[string]interface{}{}
	r = db.Query(sql)
	if r.Error != nil {
		t.Fatal("Query error", sql, r)
	}
	results7 := r.MapOnR1()
	if results7["id"].(int64) != 1002 || results7["phone"] != "13800000001" {
		t.Fatal("Result error", sql, results7, r)
	}

	results8 := userInfo{innerId: 2, Tag: "abc"}
	r = db.Query(sql)
	if r.Error != nil {
		t.Fatal("Query error", sql, results8, r)
	}
	r.To(&results8)
	if results8.Id != 1002 || results8.Phone != "13800000001" || results8.innerId != 2 || results8.Tag != "abc" {
		t.Fatal("Result error", sql, results8, r)
	}

	//var results9 int
	r = db.Query(sql)
	if r.Error != nil {
		t.Fatal("Query error", sql, r)
	}
	results9 := r.IntOnR1C1()
	if results9 != 1002 {
		t.Fatal("Result error", sql, results9, r)
	}

	//t.Log("OpenConnections", db.GetOriginDB().Stats().OpenConnections)
}

func TestInsertReplaceUpdateDelete(t *testing.T) {
	db := initDB(t)
	er := db.Insert("tempUsersForDBTest", map[string]interface{}{
		"phone": 18033336666,
		"name":  "Star",
		"parents": []string{"dd","mm"},
		"time":  ":DATE_SUB(NOW(), INTERVAL 1 DAY)",
	})
	if er.Error != nil {
		t.Fatal("Insert 1 error", er)
	}
	if er.Id() != 1 {
		t.Fatal("insertId 1 error", er, er.Id())
	}

	er = db.Insert("tempUsersForDBTest", map[string]interface{}{
		"phone":  "18000000002",
		"name":   "Tom",
		"active": true,
	})
	if er.Error != nil {
		t.Fatal("Insert 2 error", er)
	}
	if er.Id() != 2 {
		t.Fatal("insertId 2 error", er, er.Id())
	}

	er = db.Update("tempUsersForDBTest", map[string]interface{}{
		"phone": "18000000222",
		"name":  "Tom Lee",
	}, "id=?", 2)
	if er.Error != nil {
		t.Fatal("Update 2 error", er)
	}
	if er.Changes() != 1 {
		t.Fatal("Update 2 num error", er, er.Changes())
	}

	er = db.Replace("tempUsersForDBTest", map[string]interface{}{
		"phone": "18000000003",
		"name":  "Amy",
	})
	if er.Error != nil {
		t.Fatal("Replace 3 error", er)
	}
	if er.Id() != 3 {
		t.Fatal("insertId 3 error", er, er.Changes())
	}

	er = db.Exec("delete from tempUsersForDBTest where id=3")
	if er.Error != nil {
		t.Fatal("Delete 3 error", er)
	}
	if er.Changes() != 1 {
		t.Fatal("Delete 3 num error", er)
	}

	er = db.Replace("tempUsersForDBTest", map[string]interface{}{
		"phone": "18000000004",
		"name":  "Jerry",
	})
	if er.Error != nil {
		t.Fatal("Replace 4 error", er)
	}
	if er.Id() != 4 {
		t.Fatal("insertId 4 error", er, er.Changes())
	}

	stmt := db.Prepare("replace into `tempUsersForDBTest` (`id`,`phone`,`name`) values (?,?,?)")
	if stmt.Error != nil {
		t.Fatal("Prepare 4 error", stmt)
	}
	er = stmt.Exec(4, "18000000004", "Jerry's Mather")
	stmt.Close()

	if er.Error != nil {
		t.Fatal("Replace 4 error", er)
	}
	if er.Id() != 4 {
		t.Fatal("insertId 4 error", er)
	}

	userList := make([]userInfo, 0)
	r := db.Query("select * from tempUsersForDBTest")
	if r.Error != nil {
		t.Fatal("Select userList error", r)
	}
	r.To(&userList)
	if strings.Split(userList[0].Time, " ")[0] != time.Now().Add(time.Hour*24*-1).Format("2006-01-02") || userList[0].Id != 1 || userList[0].Name != "Star" || userList[0].Phone != "18033336666" || userList[0].Active != false {
		t.Fatal("Select userList 0 error", userList, r)
	}
	if len(userList[0].Parents) != 2 || userList[0].Parents[0] != "dd" {
		t.Fatal("Select userList 0 Parents error", userList, r)
	}
	if strings.Split(userList[1].Time, " ")[0] != time.Now().Format("2006-01-02") || userList[1].Id != 2 || userList[1].Name != "Tom Lee" || userList[1].Phone != "18000000222" || userList[1].Active != true {
		t.Fatal("Select userList 1 error", userList, r)
	}
	if userList[2].Id != 4 || userList[2].Name != "Jerry's Mather" || userList[2].Phone != "18000000004" {
		t.Fatal("Select userList 2 error", userList, r)
	}

	finishDB(db, t)
}

func TestTransaction(t *testing.T) {
	var userList []userInfo

	db := initDB(t)
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal("Begin error", tx)
	}

	tx.Insert("tempUsersForDBTest", map[string]interface{}{
		"phone": 18033336666,
		"name":  "Star",
		"time":  ":DATE_SUB(NOW(), INTERVAL 1 DAY)",
	})

	userList = make([]userInfo, 0)
	r := db.Query("select * from tempUsersForDBTest")
	r.To(&userList)
	if r.Error != nil || len(userList) != 0 {
		t.Fatal("Select Out Of TX", userList, r)
	}

	userList = make([]userInfo, 0)
	r = tx.Query("select * from tempUsersForDBTest")
	r.To(&userList)
	if r.Error != nil || len(userList) != 1 {
		t.Fatal("Select In TX", userList, r)
	}

	tx.Rollback()

	userList = make([]userInfo, 0)
	r = db.Query("select * from tempUsersForDBTest")
	r.To(&userList)
	if r.Error != nil || len(userList) != 0 {
		t.Fatal("Select When Rollback", userList, r)
	}

	tx = db.Begin()
	defer func() {
		if err := tx.Finish(); err != nil {
			t.Error("tx rollback error", err)
		}
		finishDB(db, t)
	}()
	if tx.Error != nil {
		t.Fatal("Begin 2 error", tx)
	}

	stmt := tx.Prepare("insert into `tempUsersForDBTest` (`id`,`phone`,`name`) values (?,?,?)")
	if stmt.Error != nil {
		t.Fatal("Prepare 4 error", r)
	}
	stmt.Exec(4, "18000000004", "Jerry's Mather")
	stmt.Close()

	tx.Commit()

	userList = make([]userInfo, 0)
	r = db.Query("select * from tempUsersForDBTest")
	r.To(&userList)
	if r.Error != nil || len(userList) != 1 {
		t.Fatal("Select When Commit", userList, r)
	}
}

func initDB(t *testing.T) *DB {
	db := GetDB(dbset, log.New(u.ShortUniqueId()))
	if db.Error != nil {
		t.Fatal("GetDB error", db)
		return nil
	}

	finishDB(db, t)
	er := db.Exec(`CREATE TABLE IF NOT EXISTS tempUsersForDBTest (
				id INT NOT NULL AUTO_INCREMENT,
				name VARCHAR(45) NOT NULL,
				phone VARCHAR(45),
				email VARCHAR(45),
				parents JSON,
				active TINYINT NOT NULL DEFAULT 0,
				time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id));`)
	if er.Error != nil {
		t.Fatal("Failed to create table", er)
	}
	return db
}

func finishDB(db *DB, t *testing.T) {
	er := db.Exec(`DROP TABLE IF EXISTS tempUsersForDBTest;`)
	if er.Error != nil {
		t.Fatal("Failed to create table", er)
	}
}

func BenchmarkForPool(b *testing.B) {

	b.StopTimer()
	sql := "SELECT 1002 id, '13800000001' phone"
	db := GetDB(dbset, nil)
	if db.Error != nil {
		b.Fatal("GetDB error", db)
		return
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		results1 := make([]map[string]interface{}, 0)
		r := db.Query(sql)
		if r.Error != nil {
			b.Fatal("Query error", sql, results1, r)
		}
		r.To(&results1)
		if results1[0]["id"].(int64) != 1002 || results1[0]["phone"].(string) != "13800000001" {
			b.Fatal("Result error", sql, results1, r)
		}
	}
	b.Log("OpenConnections", db.GetOriginDB().Stats().OpenConnections)
}

func BenchmarkForPoolParallel(b *testing.B) {

	b.StopTimer()
	sql := "SELECT 1002 id, '13800000001' phone"
	db := GetDB(dbset, nil)
	if db.Error != nil {
		b.Fatal("GetDB error", db)
		return
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			results1 := make([]map[string]interface{}, 0)
			r := db.Query(sql)
			if r.Error != nil {
				b.Fatal("Query error", sql, results1, r)
			}
			r.To(&results1)
			if results1[0]["id"].(int64) != 1002 || results1[0]["phone"].(string) != "13800000001" {
				b.Fatal("Result error", sql, results1, r)
			}
		}
	})
	b.Log("OpenConnections", db.GetOriginDB().Stats().OpenConnections)
}
