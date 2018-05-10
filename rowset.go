package impalathing

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/MediaMath/impalathing/services/beeswax"
	impala "github.com/MediaMath/impalathing/services/impalaservice"
)

type rowSet struct {
	ctx     context.Context
	client  *impala.ImpalaServiceClient
	handle  *beeswax.QueryHandle
	options Options

	offset  int
	rowSet  *beeswax.Results
	hasMore bool
	ready   bool

	metadata *beeswax.ResultsMetadata

	nextRow []string
}

// A RowSet represents an asyncronous hive operation. You can
// Reattach to a previously submitted hive operation if you
// have a valid thrift client, and the serialized Handle()
// from the prior operation.
type RowSet interface {
	Columns() []string
	Next() bool
	Scan(dest ...interface{}) error
	GetRow() ([]string, error)
	Poll() (*Status, error)
	Wait() (*Status, error)
	FetchAll() []map[string]interface{}
	MapScan(dest map[string]interface{}) error
	Handle() *beeswax.QueryHandle
}

// Represents job status, including success state and time the
// status was updated.
type Status struct {
	state beeswax.QueryState
	Error error
}

func newRowSet(ctx context.Context, client *impala.ImpalaServiceClient, handle *beeswax.QueryHandle, options Options) RowSet {
	return &rowSet{ctx: ctx, client: client, handle: handle, options: options, offset: 0, rowSet: nil,
		hasMore: true, ready: false, metadata: nil, nextRow: nil}
}

//
//
//
func (s *Status) IsSuccess() bool {
	return s.state != beeswax.QueryState_EXCEPTION
}

func (s *Status) IsComplete() bool {
	return s.state == beeswax.QueryState_FINISHED
}

func (r *rowSet) Handle() *beeswax.QueryHandle {
	return r.handle
}

// Issue a thrift call to check for the job's current status.
func (r *rowSet) Poll() (*Status, error) {
	state, err := r.client.GetState(r.ctx, r.handle)

	if err != nil {
		return nil, fmt.Errorf("Error getting status: %v", err)
	}

	if state == beeswax.QueryState_EXCEPTION {
		return nil, fmt.Errorf("Exception on Impala side")
	}

	return &Status{state, nil}, nil
}

// Wait until the job is complete, one way or another, returning Status and error.
func (r *rowSet) Wait() (*Status, error) {
	for {
		status, err := r.Poll()

		if err != nil {
			return nil, err
		}

		if status.IsComplete() {
			if status.IsSuccess() {
				r.ready = true
				return status, nil
			}
			return nil, fmt.Errorf("Query failed execution: %s", status.state.String())
		}

		time.Sleep(time.Duration(r.options.PollIntervalSeconds) * time.Second)
	}
}

func (r *rowSet) waitForSuccess() error {
	if !r.ready {
		status, err := r.Wait()
		if err != nil {
			return err
		}
		if !status.IsSuccess() || !r.ready {
			return fmt.Errorf("Unsuccessful query execution: %+v", status)
		}
		if r.metadata == nil {
			r.metadata, err = r.client.GetResultsMetadata(r.ctx, r.handle)
			if err != nil {
				log.Printf("GetResultsMetadata failed: %v\n", err)
			}
		}
	}

	return nil
}

// Prepares a row for scanning into memory, by reading data from hive if
// the operation is successful, blocking until the operation is
// complete, if necessary.
// Returns true is a row is available to Scan(), and false if the
// results are empty or any other error occurs.
func (r *rowSet) Next() bool {
	if err := r.waitForSuccess(); err != nil {
		return false
	}

	if r.rowSet == nil || r.offset >= len(r.rowSet.Data) {
		if !r.hasMore {
			return false
		}

		resp, err := r.client.Fetch(r.ctx, r.handle, false, 1000000)
		if err != nil {
			log.Printf("FetchResults failed: %v\n", err)
			return false
		}
		r.hasMore = resp.HasMore

		r.rowSet = resp
		r.offset = 0

		// We assume that if we get 0 results back, that's the end
		if len(resp.Data) == 0 {
			return false
		}
	}

	r.nextRow = strings.Split(r.rowSet.Data[r.offset], "\t")
	r.offset++

	return true
}

// Scan the last row prepared via Next() into the destination(s) provided,
// which must be pointers to value types, as in database.sql. Further,
// only pointers of the following types are supported:
//  - int, int16, int32, int64
//  - string, []byte
//  - float64
//   - bool
func (r *rowSet) Scan(dest ...interface{}) error {
	// TODO: Add type checking and conversion between compatible
	// types where possible, as well as some common error checking,
	// like passing nil. database/sql's method is very convenient,
	// for example: http://golang.org/src/pkg/database/sql/convert.go, like 85
	if r.nextRow == nil {
		return errors.New("No row to scan! Did you call Next() first?")
	}

	if len(dest) != len(r.nextRow) {
		return fmt.Errorf("Can't scan into %d arguments with input of length %d", len(dest), len(r.nextRow))
	}

	for i, val := range r.nextRow {
		d := dest[i]
		switch dt := d.(type) {
		case *string:
			*dt = val
		case *int:
			i, _ := strconv.ParseInt(val, 10, 0)
			*dt = int(i)
		case *int64:
			i, _ := strconv.ParseInt(val, 10, 0)
			*dt = int64(i)
		case *int32:
			i, _ := strconv.ParseInt(val, 10, 0)
			*dt = int32(i)
		case *int16:
			i, _ := strconv.ParseInt(val, 10, 0)
			*dt = int16(i)
		case *float64:
			*dt, _ = strconv.ParseFloat(val, 64)
			/*
			   case *[]byte:
			       *dt = []byte(val.(string))
			   case *bool:
			       *dt = val.(bool)
			*/
		default:
			return fmt.Errorf("Can't scan value of type %T with value %v", dt, val)
		}
	}

	return nil
}

//Convert from a hive column type to a Go type
func (r *rowSet) convertRawValue(raw string, hiveType string) (interface{}, error) {
	switch hiveType {
	case "string":
		return raw, nil
	case "int", "tinyint", "smallint":
		i, err := strconv.ParseInt(raw, 10, 0)
		return int32(i), err
	case "bigint":
		i, err := strconv.ParseInt(raw, 10, 0)
		return int64(i), err
	case "float", "double", "decimal":
		i, err := strconv.ParseFloat(raw, 64)
		return i, err
	case "timestamp":
		i, err := time.Parse("2006-01-02 15:04:05", raw)
		return i, err
	case "boolean":
		return raw == "true", nil
	default:
		return nil, errors.New(fmt.Sprintf("Invalid hive type %v", hiveType))
	}
}

//Fetch all rows and convert to a []map[string]interface{} with
//appropriate type conversion already carried out
func (r *rowSet) FetchAll() []map[string]interface{} {
	response := make([]map[string]interface{}, 0)
	for r.Next() {
		row := make(map[string]interface{})
		for i, val := range r.nextRow {
			conv, err := r.convertRawValue(val, r.metadata.Schema.FieldSchemas[i].Type)
			if err != nil {
				fmt.Printf("%v\n", err)
			}
			row[r.metadata.Schema.FieldSchemas[i].Name] = conv
		}
		response = append(response, row)
	}
	return response
}

func (r *rowSet) GetRow() ([]string, error) {
	if r.nextRow == nil {
		return nil, errors.New("No row to scan! Did you call Next() first?")
	}
	return r.nextRow, nil
}

// Returns the names of the columns for the given operation,
// blocking if necessary until the information is available.
func (r *rowSet) Columns() []string {
	if r.metadata == nil {
		if err := r.waitForSuccess(); err != nil {
			return nil
		}
	}
	fs := r.metadata.Schema.FieldSchemas
	cl := make([]string, len(fs))

	for i, f := range fs {
		cl[i] = f.Name
	}

	return cl
}

// MapScan scans a single Row into the dest map[string]interface{}.
func (r *rowSet) MapScan(row map[string]interface{}) error {
	for i, val := range r.nextRow {
		conv, err := r.convertRawValue(val, r.metadata.Schema.FieldSchemas[i].Type)
		if err != nil {
			return err
		}
		row[r.metadata.Schema.FieldSchemas[i].Name] = conv
	}
	return nil
}
