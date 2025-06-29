package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/ssgo/u"
)

type QueryResult struct {
	rows      *sql.Rows
	Sql       *string
	Args      []interface{}
	Error     error
	logger    *dbLogger
	usedTime  float32
	completed bool
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

func (r *QueryResult) Complete() {
	if !r.completed {
		if r.rows != nil {
			r.rows.Close()
		}
		r.completed = true
	}
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

func (r *QueryResult) StringMapOnR1() map[string]string {
	result := make(map[string]string)
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

func (r *QueryResult) FloatOnR1C1() float64 {
	var result float64 = 0
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

func (r *QueryResult) ToKV(target interface{}) error {
	v := reflect.ValueOf(target)
	t := v.Type()
	for t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = v.Type()
	}

	if t.Kind() != reflect.Map {
		r.logger.LogQueryError("target not a map", *r.Sql, r.Args, r.usedTime)
		return errors.New("target not a map")
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
			return err
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

	return nil
}

func (r *QueryResult) makeResults(results interface{}, rows *sql.Rows) error {
	if rows == nil {
		return errors.New("not a valid query result")
	}

	defer func() {
		_ = rows.Close()
		r.completed = true
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
				//fmt.Println("=====1", publicColName, found)
				if found {
					valuePtr := reflect.ValueOf(scanValues[colIndex]).Elem()
					if !valuePtr.IsNil() {
						// fmt.Println("=====2", field.Type.String(), valuePtr.String(), valuePtr.Elem().Kind(), data.FieldByName(publicColName).Kind(), valuePtr.Elem().Interface())
						if field.Type.String() == "time.Time" {
							// 转换时间
							tm, err := time.Parse("2006-01-02 15:04:05.000000", valuePtr.Elem().String())
							if err != nil {
								tm, err = time.Parse("2006-01-02 15:04:05", valuePtr.Elem().String())
							}
							if err == nil {
								data.FieldByName(publicColName).Set(reflect.ValueOf(tm))
							}
						} else if valuePtr.Elem().Kind() != data.FieldByName(publicColName).Kind() && data.FieldByName(publicColName).Kind() != reflect.Interface {
							if data.FieldByName(publicColName).Kind() == reflect.Ptr {
								//fmt.Println("=====9", data.FieldByName(publicColName).Type().Elem().Kind())
							}
							if data.FieldByName(publicColName).Kind() == reflect.Ptr && valuePtr.Elem().Kind() == data.FieldByName(publicColName).Type().Elem().Kind() {
								// 匹配指针类型
								if valuePtr.Elem().CanAddr() {
									//fmt.Println("=====5", data.FieldByName(publicColName).Type(), valuePtr.Elem().Type())
									//data.FieldByName(publicColName).Set(valuePtr.Elem().Addr())
									if data.FieldByName(publicColName).Type().AssignableTo(valuePtr.Elem().Type()) {
										// 类型完全匹配
										data.FieldByName(publicColName).Set(valuePtr.Elem().Addr())
									} else if valuePtr.Elem().Type().String() == "string" {
										// 处理字符串类型
										strVal := fixValue(col.DatabaseTypeName(), valuePtr.Elem())
										data.FieldByName(publicColName).Set(reflect.New(data.FieldByName(publicColName).Type().Elem()))
										data.FieldByName(publicColName).Elem().SetString(u.String(strVal.Interface()))
										//} else if strings.Contains(valuePtr.Elem().Type().String(), "uint") {
									} else if strings.Contains(data.FieldByName(publicColName).Type().String(), "uint") {
										// 处理整数类型
										data.FieldByName(publicColName).Set(reflect.New(data.FieldByName(publicColName).Type().Elem()))
										data.FieldByName(publicColName).Elem().SetUint(u.Uint64(valuePtr.Elem().Interface()))
										//} else if strings.Contains(valuePtr.Elem().Type().String(), "int") {
									} else if strings.Contains(data.FieldByName(publicColName).Type().String(), "int") {
										// 处理整数类型
										data.FieldByName(publicColName).Set(reflect.New(data.FieldByName(publicColName).Type().Elem()))
										data.FieldByName(publicColName).Elem().SetInt(u.Int64(valuePtr.Elem().Interface()))
										//} else if strings.Contains(valuePtr.Elem().Type().String(), "float") {
									} else if strings.Contains(data.FieldByName(publicColName).Type().String(), "float") {
										// 处理整数类型
										data.FieldByName(publicColName).Set(reflect.New(data.FieldByName(publicColName).Type().Elem()))
										data.FieldByName(publicColName).Elem().SetFloat(u.Float64(valuePtr.Elem().Interface()))
									} else {
										// TODO 是否有其他特俗情况？
										data.FieldByName(publicColName).Set(valuePtr.Elem().Addr())
									}
								}
							} else {
								// 类型不匹配
								//fmt.Println("=====3")
								convertedObject := reflect.New(data.FieldByName(publicColName).Type())
								if s, ok := valuePtr.Elem().Interface().(string); ok {
									stotedValue := new(interface{})
									if s != "" {
										err = json.Unmarshal([]byte(s), stotedValue)
										if err != nil {
											r.logger.LogError(err.Error())
										}
									}
									//fmt.Println(u.JsonP(stotedValue))
									u.Convert(stotedValue, convertedObject.Interface())
									data.FieldByName(publicColName).Set(convertedObject.Elem())
								} else {
									u.Convert(valuePtr.Elem().Interface(), convertedObject.Interface())
								}
							}
						} else if field.Type.AssignableTo(valuePtr.Elem().Type()) {
							// 类型完全匹配
							if valuePtr.Elem().Kind() == reflect.String {
								data.FieldByName(publicColName).Set(fixValue(col.DatabaseTypeName(), valuePtr.Elem()))
							} else {
								data.FieldByName(publicColName).Set(valuePtr.Elem())
							}
						} else if valuePtr.Elem().Type().String() == "string" {
							// fmt.Println("=====4", col.DatabaseTypeName(), valuePtr.Elem())
							// 处理字符串类型
							// data.FieldByName(publicColName).SetString(fixValue(col.DatabaseTypeName(), valuePtr.Elem()).String())
							data.FieldByName(publicColName).Set(fixValue(col.DatabaseTypeName(), valuePtr.Elem()))
						} else if strings.Contains(valuePtr.Elem().Type().String(), "int") {
							// 处理整数类型
							data.FieldByName(publicColName).SetInt(valuePtr.Elem().Int())
						} else if strings.Contains(valuePtr.Elem().Type().String(), "float") {
							// 处理整数类型
							data.FieldByName(publicColName).SetFloat(valuePtr.Elem().Float())
						} else {
							// TODO 是否有其他特俗情况？
							// fmt.Println("=====6", col.DatabaseTypeName(), valuePtr.Elem())
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
					// fmt.Println("=====2", col.Name(), col.DatabaseTypeName(), valuePtr.Elem().Kind(), valuePtr.Elem().Interface())
					data.SetMapIndex(reflect.ValueOf(col.Name()), fixValue(col.DatabaseTypeName(), valuePtr.Elem()))
				} else {
					data.SetMapIndex(reflect.ValueOf(col.Name()), fixValue(col.DatabaseTypeName(), reflect.New(rowType.Elem()).Elem()))
				}
			}
		} else if rowType.Kind() == reflect.Slice {
			// 结果放入Slice
			data = reflect.MakeSlice(rowType, colNum, colNum)
			for colIndex, col := range colTypes {
				valuePtr := reflect.ValueOf(scanValues[colIndex]).Elem()
				if !valuePtr.IsNil() {
					data.Index(colIndex).Set(fixValue(col.DatabaseTypeName(), valuePtr.Elem()))
				} else {
					data.Index(colIndex).Set(fixValue(col.DatabaseTypeName(), reflect.New(rowType.Elem()).Elem()))
				}
			}
		} else {
			// 只返回一列结果
			valuePtr := reflect.ValueOf(scanValues[0]).Elem()
			if !valuePtr.IsNil() {
				data = fixValue(colTypes[0].DatabaseTypeName(), valuePtr.Elem())
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

// fix datetime value for sqlite
func fixValue(colType string, v reflect.Value) reflect.Value {
	if v.Kind() == reflect.String {
		switch colType {
		case "DATE":
			str := v.String()
			if len(str) >= 10 && str[4] == '-' && str[7] == '-' {
				return reflect.ValueOf(str[:10])
			}
		case "DATETIME":
			str := v.String()
			if len(str) >= 19 && str[10] == 'T' && str[4] == '-' && str[7] == '-' && str[13] == ':' && str[16] == ':' {
				str = strings.TrimRight(str, "Z")
				if len(str) > 19 && str[19] == '.' {
					return reflect.ValueOf(str[:10] + " " + str[11:])
				}
				return reflect.ValueOf(str[:10] + " " + str[11:19])
			}
		case "TIME":
			str := v.String()
			if len(str) >= 8 && str[2] == ':' && str[4] == ':' {
				if len(str) >= 15 && str[8] == '.' {
					return reflect.ValueOf(str[0:15])
				}
				return reflect.ValueOf(str[0:8])
			}
		}
	}
	return v
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

	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return new(*[]byte)
	}

	return new(*string)
}
