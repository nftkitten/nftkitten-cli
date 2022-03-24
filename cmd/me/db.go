package me

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/reactivex/rxgo/v2"
	"go.uber.org/ratelimit"
)

var dbRateLimit ratelimit.Limiter = ratelimit.New(5) // per second

func sqlForUpsert(
	obj string,
	id_prefix string,
	values ...interface{},
) queryCommand {
	return queryCommand{
		text: fmt.Sprint(`INSERT INTO me_`, obj, `(`, id_prefix, `id,data)VALUES($1::text,$2::jsonb)
ON CONFLICT(`, id_prefix, `id)WHERE data@>$2::jsonb AND $2::jsonb@>data
DO UPDATE SET data=$2::jsonb,updated_at=now()`),
		values: values,
	}
}

func sqlForUpdate(
	obj string,
	id_prefix string,
	field string,
	values ...interface{},
) queryCommand {
	return queryCommand{
		text: fmt.Sprint(`UPDATE me_`, obj, ` SET data = jsonb_set(data, '{`, field, `}, $2::jsonb),updated_at=now()
WHERE id=$1::text`),
		values: values,
	}
}

// func sqlForUpsertScanLog(scanId string) queryCommand {
// 	return queryCommand{
// 		text: fmt.Sprint(`INSERT INTO me_scan_log(id,scanned_at)VALUES($1::text,now())
// ON CONFLICT(id) DO UPDATE SET scanned_at=now()`),
// 		values: []interface{}{scanId},
// 	}
// }

func sqlForUpsertWithParent(
	parent string,
	obj string,
	values ...interface{},
) queryCommand {
	return queryCommand{
		text: fmt.Sprint(`INSERT INTO me_`, parent, `_`, obj, `(id,`, parent, `_id,data)VALUES($3::text,$1::text,$2::jsonb)
ON CONFLICT(`, parent, `_id,id)WHERE data@>$2::jsonb AND $2::jsonb@>data
DO UPDATE SET data=$2::jsonb,updated_at=now()`),
		values: values,
	}
}

func printCommand(command queryCommand) {
	valueOutput := fmt.Sprint(command.values)
	if len(valueOutput) > 50 {
		log.Println(valueOutput[:50], command.text)
	} else {
		log.Println(valueOutput, command.text)
	}
}

func dbExecute(command queryCommand) (sql.Result, error) {
	dbRateLimit.Take()
	args := command.values
	stmt, err := db.Prepare(command.text)

	if err != nil {
		printCommand(command)
		return nil, err
	}
	res, err := stmt.Exec(args...)
	if err != nil {
		printCommand(command)
		return nil, err
	}
	return res, nil
}

func dbQuery(text string) ([]map[string]interface{}, error) {
	dbRateLimit.Take()
	rows, err := db.Query(text)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	count := len(columns)
	results := make([]map[string]interface{}, 0)

	defValues := make([]interface{}, count)
	for i := 0; i < count; i++ {
		switch columnTypes[i].DatabaseTypeName() {
		case "BOOL":
			defValues[i] = new(sql.NullBool)
		case "BIT", "VARBIT":
			defValues[i] = new(sql.NullByte)
		case "DECIMAL", "FLOAT8", "FLOAT4", "MONEY", "NUMERIC", "REAL":
			defValues[i] = new(sql.NullFloat64)
		case "INT2", "SERIAL2":
			defValues[i] = new(sql.NullInt32)
		case "INT", "INT4", "INT8", "SERIAL", "SERIAL4", "SERIAL8":
			defValues[i] = new(sql.NullInt64)
		case "TIMETZ", "TIME":
			defValues[i] = new(sql.NullTime)
		default:
			defValues[i] = new(sql.NullString)
		}
	}

	scanArgs := make([]interface{}, count)
	for rows.Next() {
		copy(scanArgs, defValues)
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i := 0; i < count; i++ {
			if val, ok := scanArgs[i].(sql.NullBool); ok {
				row[columns[i]] = val.Bool
			} else if val, ok := scanArgs[i].(sql.NullByte); ok {
				row[columns[i]] = val.Byte
			} else if val, ok := scanArgs[i].(sql.NullFloat64); ok {
				row[columns[i]] = val.Float64
			} else if val, ok := scanArgs[i].(sql.NullInt32); ok {
				row[columns[i]] = val.Int32
			} else if val, ok := scanArgs[i].(sql.NullInt64); ok {
				row[columns[i]] = val.Int64
			} else if val, ok := scanArgs[i].(sql.NullTime); ok {
				row[columns[i]] = val.Time
			} else if val, ok := scanArgs[i].(sql.NullString); ok {
				row[columns[i]] = val.String
			} else {
				row[columns[i]] = nil
			}
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return results, nil
}

// func dbQueryScanLog() map[string]bool {
// 	cmd := `SELECT id FROM me_scan_log`
// 	rows, err := dbQuery(cmd)
// 	if err != nil {
// 		panic(err)
// 	}
// 	res := make(map[string]bool)
// 	for _, row := range rows {
// 		res[fmt.Sprint(row["id"])] = true
// 	}
// 	return res
// }

func dbQueryIdSet(text string) map[string]bool {
	rows, err := dbQuery(text)
	if err != nil {
		panic(err)
	}
	values := make(map[string]bool)
	for _, row := range rows {
		values[fmt.Sprint(row["id"])] = true
	}
	return values
}

func dbExecuteMany(commands ...queryCommand) rxgo.Observable {
	var pub = make(chan rxgo.Item)
	go func() {
		for _, command := range commands {
			res, err := dbExecute(command)
			pub <- rxgo.Item{V: res, E: err}
		}
		close(pub)
	}()
	return rxgo.FromChannel(pub)
}
