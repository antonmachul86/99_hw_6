package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	IntType     = "INT"
	FloatType   = "FLOAT"
	TextType    = "TEXT"
	VarcharType = "VARCHAR"
)

type Table struct {
	Name    string
	Columns map[string]*Column
}

func (t *Table) Primary() *Column {
	for _, column := range t.Columns {
		if column.IsPrimary {
			return column
		}
	}
	return nil
}

type Column struct {
	Name            string
	Type            string
	IsNullable      bool
	IsPrimary       bool
	IsAutoIncrement bool
}

func (c *Column) DefaultValue() interface{} {
	switch c.Type {
	case IntType:
		return 0
	case FloatType:
		return 0.0
	case VarcharType, TextType:
		return ""
	}
	return nil
}

type Record map[string]interface{}

func getTables(db *sql.DB) ([]*Table, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("getTables QueryContext error  %v", err)
	}

	tables := make([]*Table, 0)
	for rows.Next() {
		table := new(Table)
		if err = rows.Scan(&table.Name); err != nil {
			return nil, fmt.Errorf("getTables QueryContext Scan error %v", err)
		}
		tables = append(tables, table)
	}

	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("getTables QueryContext Close error %v", err)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("getTables QueryContext Err error %v", err)
	}

	return tables, nil
}

func getColumns(db *sql.DB, tableName string) (map[string]*Column, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SHOW COLUMNS FROM "+tableName)
	if err != nil {
		return nil, fmt.Errorf("getColumns QueryContext error %v", err)
	}

	columns := make(map[string]*Column)
	for rows.Next() {
		var (
			null  string
			key   sql.NullString
			extra sql.NullString
		)

		column := new(Column)
		vals := []interface{}{&column.Name, &column.Type, &null, &key, new(sql.RawBytes), &extra}
		if err = rows.Scan(vals...); err != nil {
			return nil, fmt.Errorf("getColumns Scan error %v", err)
		}

		if typeParts := strings.SplitN(column.Type, "(", 2); len(typeParts) != 0 {
			column.Type = strings.ToUpper(typeParts[0])
		}
		switch column.Type {
		case IntType, FloatType, VarcharType, TextType:
		default:
			return nil, fmt.Errorf("unknown column type %s", column.Type)
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

		columns[column.Name] = column
	}

	if err = rows.Close(); err != nil {
		return nil, fmt.Errorf("getColumns Close error %v", err)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("getColumns Err error %v", err)
	}

	return columns, nil
}

type BaseQueryBuilder struct {
	builder strings.Builder
	args    []interface{}
}

func (b *BaseQueryBuilder) Where(op, column string, arg interface{}, wasWhere *bool) {
	b.builder.WriteRune(' ')
	if !*wasWhere {
		b.builder.WriteString("WHERE")
		*wasWhere = true
	} else {
		b.builder.WriteString(op)
	}
	b.builder.WriteString(fmt.Sprintf(" `%s` = ?", column))
	b.args = append(b.args, arg)
}

func (b *BaseQueryBuilder) Args() []interface{} {
	return b.args
}

func (b *BaseQueryBuilder) String() string {
	return b.builder.String()
}

type SelectQueryBuilder struct {
	BaseQueryBuilder
	wasWhere bool
}

func (b *SelectQueryBuilder) Select(table string) *SelectQueryBuilder {
	b.builder.WriteString(fmt.Sprintf("SELECT * FROM `%s`", table))
	return b
}
func (b *SelectQueryBuilder) Where(op, column string, arg interface{}) *SelectQueryBuilder {
	b.BaseQueryBuilder.Where(op, column, arg, &b.wasWhere)
	return b
}
func (b *SelectQueryBuilder) Limit(val int) *SelectQueryBuilder {
	b.builder.WriteString(fmt.Sprintf(" LIMIT %d", val))
	return b
}
func (b *SelectQueryBuilder) Offset(val int) *SelectQueryBuilder {
	b.builder.WriteString(fmt.Sprintf(" OFFSET %d", val))
	return b
}

type InsertQueryBuilder struct {
	BaseQueryBuilder
	wasBuilt bool
}

func (b *InsertQueryBuilder) Insert(tableName string) *InsertQueryBuilder {
	b.builder.WriteString(fmt.Sprintf("INSERT INTO `%s` (", tableName))
	return b
}
func (b *InsertQueryBuilder) Add(columnName string, arg interface{}) *InsertQueryBuilder {
	if len(b.args) != 0 {
		b.builder.WriteString(", ")
	}
	b.builder.WriteString(fmt.Sprintf("`%s`", columnName))
	b.args = append(b.args, arg)
	return b
}
func (b *InsertQueryBuilder) String() string {
	if !b.wasBuilt {
		b.builder.WriteString(") VALUES (")
		for i := range b.args {
			if i != 0 {
				b.builder.WriteString(", ")
			}
			b.builder.WriteRune('?')
		}
		b.builder.WriteRune(')')
		b.wasBuilt = true
	}
	return b.builder.String()
}

type UpdateQueryBuilder struct {
	BaseQueryBuilder
	wasSet   bool
	wasWhere bool
}

func (b *UpdateQueryBuilder) Table(tableName string) *UpdateQueryBuilder {
	b.builder.WriteString(fmt.Sprintf("UPDATE `%s`", tableName))
	return b
}
func (b *UpdateQueryBuilder) Set(columnName string, arg interface{}) *UpdateQueryBuilder {
	if !b.wasSet {
		b.builder.WriteString(" SET")
		b.wasSet = true
	} else {
		b.builder.WriteRune(',')
	}
	b.builder.WriteString(fmt.Sprintf(" `%s` = ?", columnName))
	b.args = append(b.args, arg)
	return b
}
func (b *UpdateQueryBuilder) Where(op, columnName string, arg interface{}) *UpdateQueryBuilder {
	b.BaseQueryBuilder.Where(op, columnName, arg, &b.wasWhere)
	return b
}

type DeleteQueryBuilder struct {
	BaseQueryBuilder
	wasWhere bool
}

func (b *DeleteQueryBuilder) Delete(tableName string) *DeleteQueryBuilder {
	b.builder.WriteString(fmt.Sprintf("DELETE FROM `%s`", tableName))
	return b
}
func (b *DeleteQueryBuilder) Where(op, columnName string, arg interface{}) *DeleteQueryBuilder {
	b.BaseQueryBuilder.Where(op, columnName, arg, &b.wasWhere)
	return b
}

type ApiError struct {
	Status int
	Err    error
}

var (
	errTableNotFound    = ApiError{Status: http.StatusNotFound, Err: errors.New("errTableNotFound")}
	errRecordNotFound   = ApiError{Status: http.StatusNotFound, Err: errors.New("errRecordNotFound")}
	errResourceNotFound = ApiError{Status: http.StatusNotFound, Err: errors.New("errResourceNotFound")}
	errNotAllowed       = ApiError{Status: http.StatusMethodNotAllowed, Err: errors.New("errNotAllowed")}
	errInternal         = ApiError{Status: http.StatusInternalServerError, Err: errors.New("errInternal")}
)

func (ae ApiError) Error() string {
	return ae.Err.Error()
}

const (
	defaultLimit  = 5
	defaultOffset = 0
)

type ResponseBody struct {
	Error    string      `json:"error,omitempty"`
	Response interface{} `json:"response,omitempty"`
}

type ResponseTablesBody struct {
	Tables []string `json:"tables"`
}

type ResponseRecordsBody struct {
	Records []Record `json:"records"`
}

type ResponseRecordBody struct {
	Record Record `json:"record"`
}

type TableHandler struct {
	db *sql.DB

	tables    []*Table
	tablesMap map[string]*Table

	listRegex   *regexp.Regexp
	detailRegex *regexp.Regexp
}

func (t *TableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	url := req.URL.Path

	switch {
	case url == "/":
		t.handleTableList(w)
	case t.listRegex.MatchString(url):
		groups := t.listRegex.FindStringSubmatch(url)
		if len(groups) != 2 {
			log.Println("ServeHTTP groups != 2 err")
			t.logAndRespond(w, http.StatusInternalServerError, errInternal)
			return
		}

		tableName := groups[1]
		table, exist := t.tablesMap[tableName]
		if !exist {
			t.logAndRespond(w, http.StatusNotFound, errTableNotFound)
			return
		}

		switch req.Method {
		case http.MethodGet:
			t.handleRecordList(w, req, table)
		case http.MethodPut:
			t.handleRecordCreate(w, req, table)
		default:
			t.logAndRespond(w, http.StatusMethodNotAllowed, errNotAllowed)
		}
	case t.detailRegex.MatchString(url):
		groups := t.detailRegex.FindStringSubmatch(url)
		if len(groups) != 3 {
			log.Println("ServeHTTP groups != 2 err")
			t.logAndRespond(w, http.StatusInternalServerError, errInternal)
			return
		}

		tableName := groups[1]
		table, exist := t.tablesMap[tableName]
		if !exist {
			t.logAndRespond(w, http.StatusNotFound, errTableNotFound)
			return
		}
		recordID, _ := strconv.Atoi(groups[2])

		switch req.Method {
		case http.MethodGet:
			t.handleRecordDetail(w, req, table, recordID)
		case http.MethodPost:
			t.handleRecordUpdate(w, req, table, recordID)
		case http.MethodDelete:
			t.handleRecordDelete(w, req, table, recordID)
		default:
			t.logAndRespond(w, http.StatusMethodNotAllowed, errNotAllowed)
		}
	default:
		t.logAndRespond(w, http.StatusNotFound, errResourceNotFound)
	}
}

func closeRows(rows *sql.Rows) error {
	if err := rows.Close(); err != nil {
		return fmt.Errorf("closeRows Close error %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("closeRows Err error %w", err)
	}
	return nil
}

func (t *TableHandler) respond(w http.ResponseWriter, status int, data interface{}, err error) {
	resp := ResponseBody{}
	if err != nil {
		if apiErr, ok := err.(ApiError); ok {
			resp.Error = apiErr.Err.Error()
			status = apiErr.Status
		} else {
			resp.Error = err.Error()
		}
	} else {
		resp.Response = data
	}
	payload, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func (t *TableHandler) handleTableList(w http.ResponseWriter) {
	tableNames := make([]string, 0, len(t.tables))
	for _, table := range t.tables {
		tableNames = append(tableNames, table.Name)
	}
	t.respond(w, http.StatusOK, ResponseTablesBody{Tables: tableNames}, nil)
}

func (t *TableHandler) handleRecordList(w http.ResponseWriter, req *http.Request, table *Table) {
	query := req.URL.Query()
	var err error
	limit := defaultLimit
	if limitStr := query.Get("limit"); limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			limit = defaultLimit
		}
	}
	offset := defaultOffset
	if offsetStr := query.Get("offset"); offsetStr != "" {
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			offset = defaultOffset
		}
	}
	builder := &SelectQueryBuilder{}
	builder.Select(table.Name).Limit(limit).Offset(offset)
	rows, err := t.db.QueryContext(req.Context(), builder.String())
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	defer closeRows(rows)
	records, err := t.createRecordsFromRows(rows)
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	t.respond(w, http.StatusOK, ResponseRecordsBody{Records: records}, nil)
}

func (t *TableHandler) handleRecordCreate(w http.ResponseWriter, req *http.Request, table *Table) {
	var record Record
	if err := json.NewDecoder(req.Body).Decode(&record); err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	defer func() { _ = req.Body.Close() }()
	builder := InsertQueryBuilder{}
	builder.Insert(table.Name)
	for _, column := range table.Columns {
		if column.IsAutoIncrement {
			continue
		}
		val := record[column.Name]
		if val == nil {
			if !column.IsNullable {
				builder.Add(column.Name, column.DefaultValue())
			}
			continue
		}
		val, err := t.validateColumnValue(column, val)
		if err != nil {
			t.logAndRespond(w, http.StatusBadRequest, ApiError{Status: http.StatusBadRequest, Err: err})
			return
		}
		builder.Add(column.Name, val)
	}
	result, err := t.db.ExecContext(req.Context(), builder.String(), builder.Args()...)
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	body := map[string]int64{table.Primary().Name: lastInsertID}
	t.respond(w, http.StatusOK, body, nil)
}

func (t *TableHandler) handleRecordDetail(w http.ResponseWriter, req *http.Request, table *Table, recordID int) {
	builder := &SelectQueryBuilder{}
	builder.Select(table.Name).Where("", table.Primary().Name, recordID).Limit(1)
	rows, err := t.db.QueryContext(req.Context(), builder.String(), builder.Args()...)
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	defer closeRows(rows)
	records, err := t.createRecordsFromRows(rows)
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	if len(records) == 0 {
		t.logAndRespond(w, http.StatusNotFound, errRecordNotFound)
		return
	}
	t.respond(w, http.StatusOK, ResponseRecordBody{Record: records[0]}, nil)
}

func (t *TableHandler) handleRecordUpdate(w http.ResponseWriter, req *http.Request, table *Table, recordID int) {
	var record Record
	if err := json.NewDecoder(req.Body).Decode(&record); err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	defer func() { _ = req.Body.Close() }()
	builder := &UpdateQueryBuilder{}
	builder.Table(table.Name)
	for _, column := range table.Columns {
		val, exist := record[column.Name]
		if !exist {
			continue
		}
		if column.IsPrimary && val != nil {
			err := ApiError{
				Status: http.StatusBadRequest,
				Err:    fmt.Errorf("invalid type  %s field ", column.Name),
			}
			t.logAndRespond(w, http.StatusBadRequest, err)
			return
		}
		val, err := t.validateColumnValue(column, val)
		if err != nil {
			t.logAndRespond(w, http.StatusBadRequest, ApiError{Status: http.StatusBadRequest, Err: err})
			return
		}
		builder.Set(column.Name, val)
	}
	builder.Where("AND", table.Primary().Name, recordID)
	result, err := t.db.ExecContext(req.Context(), builder.String(), builder.Args()...)
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	body := map[string]int64{"updated": affected}
	t.respond(w, http.StatusOK, body, nil)
}

func (t *TableHandler) handleRecordDelete(w http.ResponseWriter, req *http.Request, table *Table, recordID int) {
	builder := &DeleteQueryBuilder{}
	builder.Delete(table.Name).Where("AND", table.Primary().Name, recordID)
	result, err := t.db.ExecContext(req.Context(), builder.String(), builder.Args()...)
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.logAndRespond(w, http.StatusInternalServerError, errInternal)
		return
	}
	body := map[string]int64{"deleted": affected}
	t.respond(w, http.StatusOK, body, nil)
}

func (t *TableHandler) createRecordsFromRows(rows *sql.Rows) ([]Record, error) {
	columnsTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0)
	for rows.Next() {
		vals := make([]interface{}, len(columnsTypes))
		for i, columnType := range columnsTypes {
			dbType := columnType.DatabaseTypeName()
			if nullable, _ := columnType.Nullable(); nullable {
				switch dbType {
				case IntType:
					vals[i] = new(sql.NullInt64)
				case FloatType:
					vals[i] = new(sql.NullFloat64)
				case VarcharType, TextType:
					vals[i] = new(sql.NullString)
				}
			} else {
				switch dbType {
				case IntType:
					vals[i] = new(int64)
				case FloatType:
					vals[i] = new(float64)
				case VarcharType, TextType:
					vals[i] = new(string)
				}
			}
		}
		record := make(Record, len(columnsTypes))
		if err = rows.Scan(vals...); err != nil {
			return nil, err
		}
		for i, columnType := range columnsTypes {
			if valuer, ok := vals[i].(driver.Valuer); ok {
				val, err := valuer.Value()
				if err != nil {
					return nil, err
				}
				record[columnType.Name()] = val
			} else {
				record[columnType.Name()] = vals[i]
			}
		}
		records = append(records, record)
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	return records, nil
}

func (t *TableHandler) validateColumnValue(column *Column, val interface{}) (interface{}, error) {
	if column.IsNullable && val == nil {
		return val, nil
	}
	switch column.Type {
	case IntType:
		switch v := val.(type) {
		case float64:
			return int(v), nil
		case int:
			return v, nil
		default:
			return nil, fmt.Errorf("invalid type of %s field ", column.Name)
		}
	case FloatType:
		switch v := val.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		default:
			return nil, fmt.Errorf("invalid type of %s field", column.Name)
		}
	case VarcharType, TextType:
		if s, ok := val.(string); ok {
			return s, nil
		}
		return nil, fmt.Errorf("invalid type of %s field", column.Name)
	}
	return val, nil
}

func (t *TableHandler) logAndRespond(w http.ResponseWriter, status int, err error) {
	log.Println(err)
	t.respond(w, status, nil, err)
}

func getString(rec Record, key string) (string, bool) {
	v, ok := rec[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
func getInt(rec Record, key string) (int, bool) {
	v, ok := rec[key]
	if !ok {
		return 0, false
	}
	switch val := v.(type) {
	case float64:
		return int(val), true
	case int:
		return val, true
	}
	return 0, false
}
func getFloat(rec Record, key string) (float64, bool) {
	v, ok := rec[key]
	if !ok {
		return 0, false
	}
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	}
	return 0, false
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	mux := http.NewServeMux()

	tables, err := getTables(db)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %v", err)
	}

	tablesMap := make(map[string]*Table, len(tables))
	for _, table := range tables {
		columns, err := getColumns(db, table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get columns of table %s: %v", table.Name, err)
		}

		table.Columns = columns
		if table.Primary() == nil {
			return nil, fmt.Errorf("table %s doesn't have the primary key", table.Name)
		}

		tablesMap[table.Name] = table
	}

	mux.Handle("/", &TableHandler{
		db:          db,
		tables:      tables,
		tablesMap:   tablesMap,
		listRegex:   regexp.MustCompile(`^/([a-zA-Z0-9_-]+)/?$`),
		detailRegex: regexp.MustCompile(`^/([a-zA-Z0-9_-]+)/(\d+)$`),
	})
	return mux, nil
}
