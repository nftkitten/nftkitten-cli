package me

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/fatih/color"
	"github.com/tidwall/limiter"
)

var concurrentlimiter = limiter.New(20)

func sqlForUpsert(
	obj string,
	id_prefix string,
	values ...interface{},
) queryCommand {
	return queryCommand{
		text: fmt.Sprint(`INSERT INTO me_`, obj, `(`, id_prefix, `id,data,stats)VALUES($1::text,$2::jsonb,$3::jsonb)`,
			`ON CONFLICT(`, id_prefix, `id)WHERE data@>$2::jsonb AND $2::jsonb@>data OR stats@>$3::jsonb AND $3::jsonb@>stats `,
			`DO UPDATE SET data=$2::jsonb,stats=$3::jsonb,updated_at=now()`),
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
	concurrentlimiter.Begin()
	defer concurrentlimiter.End()
	if len(command.text) > 50 {
		color.New(color.FgYellow).Println(command.text[:50] + "...")
	} else {
		color.New(color.FgYellow).Println(command.text + "...")
	}
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
	concurrentlimiter.Begin()
	defer concurrentlimiter.End()
	if len(text) > 50 {
		color.New(color.FgYellow).Println(text[:50] + "...")
	} else {
		color.New(color.FgYellow).Println(text + "...")
	}
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

func dbExecuteMany(commands ...queryCommand) (res interface{}, err error) {
	for _, command := range commands {
		cmd := command
		res, err = dbExecute(cmd)
	}
	return res, err
}
