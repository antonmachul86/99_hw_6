package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

// db, err := sql.Open("mysql", DSN)
// err = db.Ping()
var (
	TypeInt     = "INT"
	TypeFloat   = "FLOAT"
	TypeText    = "TEXT"
	TypeVarchar = "VARCHAR"
)

type Table struct {
	Name    string
	Columns []*Column
	primary *Column
}
type Column struct {
	Name            string
	Type            string
	IsNullable      bool
	IsPrimary       bool
	IsAutoIncrement bool
}

func (t *Table) Primary() *Column {
	if t.primary != nil {
		return t.primary
	}
	for _, column := range t.Columns {
		if column.IsPrimary {
			t.primary = column
			return column
		}
	}
	return nil
}

type Record map[string]interface{}

func getTables(db *sql.DB) ([]*Table, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("db.QueryContext SHOW TABLES err %v", err)
	}
	tables := make([]*Table, 0)

	for rows.Next() {
		table := new(Table)
		if err = rows.Scan(&table.Name); err != nil {
			return nil, fmt.Errorf("rows.Scan err %v", err)
		}
		tables = append(tables, table)
	}

	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("rows.Close err %v", err)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err err %v", err)
	}

	return tables, nil
}

func getColumns(db *sql.DB, tableName string) ([]*Column, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW COLUMNS FROM "+tableName)
	if err != nil {
		return nil, fmt.Errorf("db.QueryContext SHOW COLUMNS FROM err %v", err)
	}
	columns := make([]*Column, 0)
	for rows.Next() {
		var (
			null  string
			key   sql.NullString
			extra sql.NullString
		)

		column := new(Column)
		vals := []interface{}{
			&column.Name,
			&column.Type,
			&null, &key,
			new(sql.RawBytes),
			&extra,
		}
		if err = rows.Scan(vals...); err != nil {
			return nil, fmt.Errorf("getColumns rows.Scan err %v", err)
		}

		if typeParts := strings.SplitN(column.Type, "(", 2); len(typeParts) != 0 {
			column.Type = strings.ToUpper(typeParts[0])
		}
		switch column.Type {
		case TypeFloat, TypeInt, TypeText, TypeVarchar:
		default:
			return nil, fmt.Errorf("getColumns switch column.Type %s", err)
		}

		if null == "YES" {
			column.IsNullable = true
		}
		if key.Valid && key.String == "PRI" {
			column.IsPrimary = true
		}
		if extra.Valid && extra.String == "auto_increment" {
			column.IsAutoIncrement = true
		}

		columns = append(columns, column)
	}

	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("getColumns rows.Close err %v", err)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("getColumns rows.Err err %v", err)
	}

	return columns, nil
}

type ConditionQueryBuilder struct {
	builder  strings.Builder
	args     []interface{}
	wasWhere bool
}

func (b *ConditionQueryBuilder) Where(op, column string, arg interface{}) *ConditionQueryBuilder {
	b.builder.WriteRune(' ')
	if !b.wasWhere {
		b.builder.WriteString("WHERE")
		b.wasWhere = true
	} else {
		b.builder.WriteString(op)
	}

	b.builder.WriteString(fmt.Sprintf(" `%s` = ?", column))
	b.args = append(b.args, arg)
	return b
}

func (b *ConditionQueryBuilder) String() string {
	return b.builder.String()
}

func (b *ConditionQueryBuilder) Args() []interface{} {
	return b.args
}

type SelectQueryBuilder struct {
	ConditionQueryBuilder
}

func (b *SelectQueryBuilder) Select(table string) *SelectQueryBuilder {
	stmt := fmt.Sprintf("SELECT * FROM `%s`", table)
	b.builder.WriteString(stmt)
	return b
}

func (b *SelectQueryBuilder) Where(op, column string, arg interface{}) *SelectQueryBuilder {
	b.ConditionQueryBuilder.Where(op, column, arg)
	return b
}

func (b *SelectQueryBuilder) Limit(val int) *SelectQueryBuilder {
	stmt := fmt.Sprintf("LIMIT %d", val)
	b.builder.WriteString(stmt)
	return b
}

type InsertQueryBuilder struct {
	builder  strings.Builder
	args     []interface{}
	wasBuilt bool
}

func (b *InsertQueryBuilder) Insert(tableName string) *InsertQueryBuilder {
	b.builder.WriteString(fmt.Sprintf("INSERT INTO `%s` (", tableName))
	return b
}

//func NewDbExplorer(db *sql.DB) (http.Handler, error) {
//
//}
