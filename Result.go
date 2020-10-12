package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/ssgo/u"
	"reflect"
	"time"
)

type QueryResult struct {
	rows     *sql.Rows
	Sql      *string
	Args     []interface{}
	Error    error
	logger   *dbLogger
	usedTime float32
}

type ExecResult struct {
	result   sql.Result
	Sql      *string
	Args     []interface{}
	Error    error
	logger   *dbLogger
	usedTime float32
}

func (r *ExecResult) Changes() int64 {
	if r.result == nil {
		return 0
	}
	numChanges, err := r.result.RowsAffected()
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
		return 0
	}
	return numChanges
}

func (r *ExecResult) Id() int64 {
	if r.result == nil {
		return 0
	}
	insertId, err := r.result.LastInsertId()
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
		return 0
	}
	return insertId
}

func (r *QueryResult) To(result interface{}) error {
	if r.rows == nil {
		return errors.New("operate on a bad query")
	}
	return r.makeResults(result, r.rows)
}

func (r *QueryResult) MapResults() []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

func (r *QueryResult) SliceResults() [][]interface{} {
	result := make([][]interface{}, 0)
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

func (r *QueryResult) StringMapResults() []map[string]string {
	result := make([]map[string]string, 0)
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

func (r *QueryResult) StringSliceResults() [][]string {
	result := make([][]string, 0)
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

func (r *QueryResult) MapOnR1() map[string]interface{} {
	result := make(map[string]interface{})
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

//func (r *QueryResult) SliceOnR1() []interface{} {
//	result := make([]interface{}, 0)
//	err := r.makeResults(&result, r.rows)
//	if err != nil {
//		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
//	}
//	return result
//}

func (r *QueryResult) IntsOnC1() []int64 {
	result := make([]int64, 0)
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

func (r *QueryResult) StringsOnC1() []string {
	result := make([]string, 0)
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

func (r *QueryResult) IntOnR1C1() int64 {
	var result int64 = 0
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

func (r *QueryResult) StringOnR1C1() string {
	result := ""
	err := r.makeResults(&result, r.rows)
	if err != nil {
		r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
	}
	return result
}

func (r *QueryResult) ToKV(target interface{}) {
	v := reflect.ValueOf(target)
	t := v.Type()
	for t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = v.Type()
	}

	if t.Kind() != reflect.Map {
		r.logger.LogQueryError("target not a map", *r.Sql, r.Args, r.usedTime)
		return
	}

	vt := t.Elem()
	finalVt := vt
	for finalVt.Kind() == reflect.Ptr {
		finalVt = finalVt.Elem()
	}
	if finalVt.Kind() == reflect.Map || finalVt.Kind() == reflect.Struct {
		colTypes, err := r.getColumnTypes()
		list := r.MapResults()
		if err != nil {
			r.logger.LogQueryError(err.Error(), *r.Sql, r.Args, r.usedTime)
			return
		} else {
			for _, item := range list {
				newKey := reflect.ValueOf(reflect.New(t.Key()).Interface()).Elem()
				u.Convert(item[colTypes[0].Name()], newKey)

				newValue := v.MapIndex(newKey)
				isNew := false
				if !newValue.IsValid() {
					newValue = reflect.New(vt)
					isNew = true
				}

				err := mapstructure.WeakDecode(item, newValue.Interface())
				if err != nil {
					r.logger.LogError(err.Error())
				}

				if isNew {
					v.SetMapIndex(newKey, newValue.Elem())
				}
			}
		}
	} else {
		list := r.SliceResults()
		for _, item := range list {
			if len(item) < 2 {
				continue
			}
			switch vt.Kind() {
			case reflect.Int:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(u.Int(item[1])))
			case reflect.Int8:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(int8(u.Int(item[1]))))
			case reflect.Int16:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(int16(u.Int(item[1]))))
			case reflect.Int32:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(int32(u.Int(item[1]))))
			case reflect.Int64:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(u.Int64(item[1])))
			case reflect.Uint:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(uint(u.Int(item[1]))))
			case reflect.Uint8:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(uint8(u.Int(item[1]))))
			case reflect.Uint16:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(uint16(u.Int(item[1]))))
			case reflect.Uint32:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(uint32(u.Int(item[1]))))
			case reflect.Uint64:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(uint64(u.Int64(item[1]))))
			case reflect.Float32:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(u.Float(item[1])))
			case reflect.Float64:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(u.Float64(item[1])))
			case reflect.Bool:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(u.Bool(item[1])))
			case reflect.String:
				v.SetMapIndex(reflect.ValueOf(u.String(item[0])), reflect.ValueOf(u.String(item[1])))
			}
		}
	}

	return
}

func (r *QueryResult) makeResults(results interface{}, rows *sql.Rows) error {
	if rows == nil {
		return errors.New("not a valid query result")
	}

	defer func() {
		_ = rows.Close()
	}()
	resultsValue := reflect.ValueOf(results)
	if resultsValue.Kind() != reflect.Ptr {
		err := fmt.Errorf("results must be a pointer")
		return err
	}

	for resultsValue.Kind() == reflect.Ptr {
		resultsValue = resultsValue.Elem()
	}
	rowType := resultsValue.Type()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	colNum := len(colTypes)
	originRowType := rowType
	if rowType.Kind() == reflect.Slice {
		// 处理数组类型，非数组类型表示只取一行数据
		rowType = rowType.Elem()
		originRowType = rowType
		for rowType.Kind() == reflect.Ptr {
			rowType = rowType.Elem()
		}
	}

	scanValues := make([]interface{}, colNum)
	if rowType.Kind() == reflect.Struct {
		// 按结构处理数据
		for colIndex, col := range colTypes {
			publicColName := makePublicVarName(col.Name())
			field, found := rowType.FieldByName(publicColName)
			if found {
				if field.Type.Kind() == reflect.Interface {
					scanValues[colIndex] = makeValue(colTypes[colIndex].ScanType())
				} else {
					scanValues[colIndex] = makeValue(field.Type)
				}
			} else {
				scanValues[colIndex] = makeValue(nil)
			}
		}
	} else if rowType.Kind() == reflect.Map {
		// 按Map处理数据
		for colIndex := range colTypes {
			if rowType.Elem().Kind() == reflect.Interface {
				scanValues[colIndex] = makeValue(colTypes[colIndex].ScanType())
			} else {
				scanValues[colIndex] = makeValue(rowType.Elem())
			}
		}
	} else if rowType.Kind() == reflect.Slice {
		// 按Map处理数据
		for colIndex := range colTypes {
			if rowType.Elem().Kind() == reflect.Interface {
				scanValues[colIndex] = makeValue(colTypes[colIndex].ScanType())
			} else {
				scanValues[colIndex] = makeValue(rowType.Elem())
			}
		}
	} else {
		// 只返回一列结果
		if rowType.Kind() == reflect.Interface {
			scanValues[0] = makeValue(colTypes[0].ScanType())
		} else {
			scanValues[0] = makeValue(rowType)
		}
		for colIndex := 1; colIndex < colNum; colIndex++ {
			scanValues[colIndex] = makeValue(nil)
		}
	}

	var data reflect.Value
	isNew := true
	for rows.Next() {

		err = rows.Scan(scanValues...)
		if err != nil {
			return err
		}
		if rowType.Kind() == reflect.Struct {
			if resultsValue.Kind() == reflect.Slice {
				data = reflect.New(rowType).Elem()
			} else {
				data = resultsValue
				isNew = false
			}
			for colIndex, col := range colTypes {
				publicColName := makePublicVarName(col.Name())
				field, found := rowType.FieldByName(publicColName)
				if found {
					valuePtr := reflect.ValueOf(scanValues[colIndex]).Elem()
					if !valuePtr.IsNil() {
						if field.Type.String() == "time.Time" {
							tm, err := time.Parse("2006-01-02 15:04:05", valuePtr.Elem().String())
							if err != nil {
								data.FieldByName(publicColName).Set(reflect.ValueOf(tm))
							}
						} else if valuePtr.Elem().Kind() != data.FieldByName(publicColName).Kind() && data.FieldByName(publicColName).Kind() != reflect.Interface {
							convertedObject := reflect.New(data.FieldByName(publicColName).Type())
							if s, ok := valuePtr.Elem().Interface().(string); ok {
								stotedValue := new(interface{})
								json.Unmarshal([]byte(s), stotedValue)
								u.Convert(stotedValue, convertedObject.Interface())
								data.FieldByName(publicColName).Set(convertedObject.Elem())
							} else {
								u.Convert(valuePtr.Elem().Interface(), convertedObject.Interface())
							}
						} else {
							data.FieldByName(publicColName).Set(valuePtr.Elem())
						}
					}
				}
			}
		} else if rowType.Kind() == reflect.Map {
			// 结果放入Map
			if resultsValue.Kind() == reflect.Slice {
				data = reflect.MakeMap(rowType)
			} else {
				data = resultsValue
				isNew = false
			}
			for colIndex, col := range colTypes {
				valuePtr := reflect.ValueOf(scanValues[colIndex]).Elem()
				if !valuePtr.IsNil() {
					data.SetMapIndex(reflect.ValueOf(col.Name()), valuePtr.Elem())
				}
			}
		} else if rowType.Kind() == reflect.Slice {
			// 结果放入Slice
			data = reflect.MakeSlice(rowType, colNum, colNum)
			for colIndex := range colTypes {
				valuePtr := reflect.ValueOf(scanValues[colIndex]).Elem()
				if !valuePtr.IsNil() {
					data.Index(colIndex).Set(valuePtr.Elem())
				}
			}
		} else {
			// 只返回一列结果
			valuePtr := reflect.ValueOf(scanValues[0]).Elem()
			if !valuePtr.IsNil() {
				data = valuePtr.Elem()
			}
		}

		if resultsValue.Kind() == reflect.Slice {
			if originRowType.Kind() == reflect.Ptr {
				resultsValue = reflect.Append(resultsValue, data.Addr())
			} else {
				resultsValue = reflect.Append(resultsValue, data)
			}
		} else {
			resultsValue = data
			break
		}
	}

	if isNew && resultsValue.IsValid() {
		reflect.ValueOf(results).Elem().Set(resultsValue)
	}
	return nil
}

func (r *QueryResult) getColumnTypes() ([]*sql.ColumnType, error) {
	if r.rows == nil {
		return nil, errors.New("not a valid query result")
	}

	return r.rows.ColumnTypes()
}

func makePublicVarName(name string) string {
	colNameBytes := []byte(name)
	if colNameBytes[0] >= 97 {
		colNameBytes[0] -= 32
		return string(colNameBytes)
	} else {
		return name
	}
}

func makeValue(t reflect.Type) interface{} {
	if t == nil {
		return new(*string)
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Int:
		return new(*int)
	case reflect.Int8:
		return new(*int8)
	case reflect.Int16:
		return new(*int16)
	case reflect.Int32:
		return new(*int32)
	case reflect.Int64:
		return new(*int64)
	case reflect.Uint:
		return new(*uint)
	case reflect.Uint8:
		return new(*uint8)
	case reflect.Uint16:
		return new(*uint16)
	case reflect.Uint32:
		return new(*uint32)
	case reflect.Uint64:
		return new(*uint64)
	case reflect.Float32:
		return new(*float32)
	case reflect.Float64:
		return new(*float64)
	case reflect.Bool:
		return new(*bool)
	case reflect.String:
		return new(*string)
	}

	return new(*string)
	//if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8{
	//	return new(string)
	//}
}
