package cassandra

// #cgo LDFLAGS: -lcassandra
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import "unsafe"
import "errors"
import "reflect"

const (
	CASS_VALUE_TYPE_UNKNOWN   = 0xFFFF
	CASS_VALUE_TYPE_CUSTOM    = 0x0000
	CASS_VALUE_TYPE_ASCII     = 0x0001
	CASS_VALUE_TYPE_BIGINT    = 0x0002
	CASS_VALUE_TYPE_BLOB      = 0x0003
	CASS_VALUE_TYPE_BOOLEAN   = 0x0004
	CASS_VALUE_TYPE_COUNTER   = 0x0005
	CASS_VALUE_TYPE_DECIMAL   = 0x0006
	CASS_VALUE_TYPE_DOUBLE    = 0x0007
	CASS_VALUE_TYPE_FLOAT     = 0x0008
	CASS_VALUE_TYPE_INT       = 0x0009
	CASS_VALUE_TYPE_TEXT      = 0x000A
	CASS_VALUE_TYPE_TIMESTAMP = 0x000B
	CASS_VALUE_TYPE_UUID      = 0x000C
	CASS_VALUE_TYPE_VARCHAR   = 0x000D
	CASS_VALUE_TYPE_VARINT    = 0x000E
	CASS_VALUE_TYPE_TIMEUUID  = 0x000F
	CASS_VALUE_TYPE_INET      = 0x0010
	CASS_VALUE_TYPE_LIST      = 0x0020
	CASS_VALUE_TYPE_MAP       = 0x0021
	CASS_VALUE_TYPE_SET       = 0x0022
)

type Cluster struct {
	cptr *C.struct_CassCluster_
}

type Future struct {
	cptr *C.struct_CassFuture_
}

type Session struct {
	cptr *C.struct_CassSession_
}

type Result struct {
	iter *C.struct_CassIterator_
	cptr *C.struct_CassResult_
}

type Prepared struct {
	cptr *C.struct_CassPrepared_
}

type Statement struct {
	cptr *C.struct_CassStatement_
}

func NewCluster() *Cluster {
	cluster := new(Cluster)
	cluster.cptr = C.cass_cluster_new()
	// defer cluster.Finalize()

	return cluster
}

func NewStatement(query string, param_count int) *Statement {
	var cass_query C.struct_CassString_
	cass_query.data = C.CString(query)
	cass_query.length = C.cass_size_t(len(query))
	defer C.free(unsafe.Pointer(cass_query.data))

	statement := new(Statement)
	statement.cptr = C.cass_statement_new(cass_query, C.cass_size_t(param_count))
	// defer statement.Finalize()
	return statement
}

func (prepared *Prepared) Bind() *Statement {
	statement := new(Statement)
	statement.cptr = C.cass_prepared_bind(prepared.cptr)
	// defer statement.Finalize()
	return statement
}

func (statement *Statement) Bind(args ...interface{}) error {
	var err C.CassError = C.CASS_OK

	for i, v := range args {

		switch v := v.(type) {

		case nil:
			err = C.cass_statement_bind_null(statement.cptr, C.cass_size_t(i))

		case int32:
			err = C.cass_statement_bind_int32(statement.cptr, C.cass_size_t(i), C.cass_int32_t(v))

		case int64:
			err = C.cass_statement_bind_int64(statement.cptr, C.cass_size_t(i), C.cass_int64_t(v))

		case float32:
			err = C.cass_statement_bind_float(statement.cptr, C.cass_size_t(i), C.cass_float_t(v))

		case float64:
			err = C.cass_statement_bind_double(statement.cptr, C.cass_size_t(i), C.cass_double_t(v))

		case bool:
			if v {
				err = C.cass_statement_bind_bool(statement.cptr, C.cass_size_t(i), 1)
			} else {
				err = C.cass_statement_bind_bool(statement.cptr, C.cass_size_t(i), 0)
			}

		case string:
			var str C.CassString
			str.data = C.CString(v)
			str.length = C.cass_size_t(len(v))
			defer C.free(unsafe.Pointer(str.data))
			err = C.cass_statement_bind_string(statement.cptr, C.cass_size_t(i), str)

		case []byte:
			var bytes C.CassBytes
			bytes.data = (*C.cass_byte_t)(unsafe.Pointer(&v))
			bytes.size = C.cass_size_t(len(v))
			err = C.cass_statement_bind_bytes(statement.cptr, C.cass_size_t(i), bytes)
		}

	}

	if err != C.CASS_OK {
		return errors.New(C.GoString(C.cass_error_desc(err)))
	}

	return nil
}

func (cluster *Cluster) Finalize() {
	C.cass_cluster_free(cluster.cptr)
	cluster.cptr = nil
}

func (future *Future) Finalize() {
	C.cass_future_free(future.cptr)
	future.cptr = nil
}

func (result *Result) Finalize() {
	C.cass_result_free(result.cptr)
	result.cptr = nil
}

func (prepared *Prepared) Finalize() {
	C.cass_prepared_free(prepared.cptr)
	prepared.cptr = nil
}

func (statement *Statement) Finalize() {
	C.cass_statement_free(statement.cptr)
	statement.cptr = nil
}

func (future *Future) Session() *Session {
	session := new(Session)
	session.cptr = C.cass_future_get_session(future.cptr)
	return session
}

func (future *Future) Result() *Result {
	result := new(Result)
	result.cptr = C.cass_future_get_result(future.cptr)
	// defer result.Finalize()
	return result
}

func (future *Future) Prepared() *Prepared {
	prepared := new(Prepared)
	prepared.cptr = C.cass_future_get_prepared(future.cptr)
	// defer prepared.Finalize()
	return prepared
}

func (future *Future) Ready() bool {
	return C.cass_future_ready(future.cptr) == C.cass_true
}

func (future *Future) Wait() {
	C.cass_future_wait(future.cptr)
}

func (future *Future) WaitTimed(timeout uint64) bool {
	return C.cass_future_wait_timed(future.cptr, C.cass_duration_t(timeout)) == C.cass_true
}

func (cluster *Cluster) SetContactPoints(contactPoints string) {
	contacts_cstr := C.CString(contactPoints)
	defer C.free(unsafe.Pointer(contacts_cstr))
	C.cass_cluster_set_contact_points(cluster.cptr, contacts_cstr)
}

func (cluster *Cluster) SetPort(port int64) {
	port_cint := C.int(port)
	C.cass_cluster_set_port(cluster.cptr, port_cint)
}

func (cluster *Cluster) Connect() *Future {
	future := new(Future)
	future.cptr = C.cass_cluster_connect(cluster.cptr)
	// defer future.Finalize()
	return future
}

func (cluster *Cluster) ConnectKeyspace(keyspace string) *Future {
	keyspace_cstr := C.CString(keyspace)
	defer C.free(unsafe.Pointer(keyspace_cstr))
	future := new(Future)
	future.cptr = C.cass_cluster_connect_keyspace(cluster.cptr, keyspace_cstr)
	// defer future.Finalize()
	return future
}

func (session *Session) Execute(statement *Statement) *Future {
	future := new(Future)
	future.cptr = C.cass_session_execute(session.cptr, statement.cptr)
	return future
}

func (result *Result) RowCount() uint64 {
	return uint64(C.cass_result_row_count(result.cptr))
}

func (result *Result) ColumnCount() uint64 {
	return uint64(C.cass_result_column_count(result.cptr))
}

func (result *Result) ColumnName(index uint64) string {
	column_name := C.cass_result_column_name(result.cptr, C.cass_size_t(index))
	return C.GoStringN(column_name.data, C.int(column_name.length))
}

func (result *Result) ColumnType(index uint64) int {
	return int(C.cass_result_column_type(result.cptr, C.cass_size_t(index)))
}

func (result *Result) HasMorePages() bool {
	return C.cass_result_has_more_pages(result.cptr) != 0
}

func (result *Result) Next() bool {
	if result.iter == nil {
		result.iter = C.cass_iterator_from_result(result.cptr)
	}
	return C.cass_iterator_next(result.iter) != 0
}

func (result *Result) Scan(args ...interface{}) error {

	if result.ColumnCount() != uint64(len(args)) {
		errors.New("invalid argument count")
	}

	row := C.cass_iterator_get_row(result.iter)

	var err C.CassError = C.CASS_OK

	for i, v := range args {
		value := C.cass_row_get_column(row, C.cass_size_t(i))

		switch v := v.(type) {

		case *string:
			var str C.CassString
			err = C.cass_value_get_string(value, &str)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = C.GoStringN(str.data, C.int(str.length))

		case *[]byte:
			var b C.CassBytes
			err = C.cass_value_get_bytes(value, &b)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = C.GoBytes(unsafe.Pointer(b.data), C.int(b.size))

		case *int32:
			var i32 C.cass_int32_t
			err = C.cass_value_get_int32(value, &i32)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = int32(i32)

		case *int64:
			var i64 C.cass_int64_t
			err = C.cass_value_get_int64(value, &i64)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = int64(i64)

		case *float32:
			var f32 C.cass_float_t
			err = C.cass_value_get_float(value, &f32)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = float32(f32)

		case *float64:
			var f64 C.cass_double_t
			err = C.cass_value_get_double(value, &f64)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = float64(f64)

		case *bool:
			var b C.cass_bool_t
			err = C.cass_value_get_bool(value, &b)
			if err != C.CASS_OK {
				return errors.New(C.GoString(C.cass_error_desc(err)))
			}
			*v = bool(b != 0)

		default:
			return errors.New("unsupported type in Scan: " + reflect.TypeOf(v).String())
		}
	}

	return nil
}
