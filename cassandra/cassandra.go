package cassandra

// #cgo CFLAGS: -I/usr/local/include
// #cgo LDFLAGS: -lcassandra -L/usr/local/lib
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import "unsafe"
import "errors"
import "reflect"

const (
	CASS_OK = 0
)

const (
	CASS_ERROR_SOURCE_NONE = iota
	CASS_ERROR_SOURCE_LIB
	CASS_ERROR_SOURCE_SERVER
	CASS_ERROR_SOURCE_SSL
	CASS_ERROR_SOURCE_COMPRESSION
)

const (
	CASS_ERROR_LIB_BAD_PARAMS = iota
	CASS_ERROR_LIB_NO_STREAMS
	CASS_ERROR_LIB_UNABLE_TO_INIT
	CASS_ERROR_LIB_MESSAGE_ENCODE
	CASS_ERROR_LIB_HOST_RESOLUTION
	CASS_ERROR_LIB_UNEXPECTED_RESPONSE
	CASS_ERROR_LIB_REQUEST_QUEUE_FULL
	CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD
	CASS_ERROR_LIB_WRITE_ERROR
	CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
	CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS
	CASS_ERROR_LIB_INVALID_ITEM_COUNT
	CASS_ERROR_LIB_INVALID_VALUE_TYPE
	CASS_ERROR_LIB_REQUEST_TIMED_OUT
	CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE
	CASS_ERROR_LIB_CALLBACK_ALREADY_SET
	CASS_ERROR_LIB_INVALID_STATEMENT_TYPE
	CASS_ERROR_LIB_NAME_DOES_NOT_EXIST
	CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL
	CASS_ERROR_LIB_NULL_VALUE
	CASS_ERROR_LIB_NOT_IMPLEMENTED
	CASS_ERROR_LIB_UNABLE_TO_CONNECT
	CASS_ERROR_LIB_UNABLE_TO_CLOSE
	CASS_ERROR_SERVER_SERVER_ERROR
	CASS_ERROR_SERVER_PROTOCOL_ERROR
	CASS_ERROR_SERVER_BAD_CREDENTIALS
	CASS_ERROR_SERVER_UNAVAILABLE
	CASS_ERROR_SERVER_OVERLOADED
	CASS_ERROR_SERVER_IS_BOOTSTRAPPING
	CASS_ERROR_SERVER_TRUNCATE_ERROR
	CASS_ERROR_SERVER_WRITE_TIMEOUT
	CASS_ERROR_SERVER_READ_TIMEOUT
	CASS_ERROR_SERVER_SYNTAX_ERROR
	CASS_ERROR_SERVER_UNAUTHORIZED
	CASS_ERROR_SERVER_INVALID_QUERY
	CASS_ERROR_SERVER_CONFIG_ERROR
	CASS_ERROR_SERVER_ALREADY_EXISTS
	CASS_ERROR_SERVER_UNPREPARED
	CASS_ERROR_SSL_INVALID_CERT
	CASS_ERROR_SSL_INVALID_PRIVATE_KEY
	CASS_ERROR_SSL_NO_PEER_CERT
	CASS_ERROR_SSL_INVALID_PEER_CERT
	CASS_ERROR_SSL_IDENTITY_MISMATCH
)

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

const (
	CASS_LOG_DISABLED = iota
	CASS_LOG_CRITICAL
	CASS_LOG_ERROR
	CASS_LOG_WARN
	CASS_LOG_INFO
	CASS_LOG_DEBUG
	CASS_LOG_TRACE
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

type Uuid struct {
	uuid C.struct_CassUuid_
}

type UuidGenerator struct {
	cptr *C.struct_CassUuidGen_
}

type Metrics struct {
	Requests struct {
		Min               int64
		Max               int64
		Mean              int64
		Stddev            int64
		Median            int64
		Percentile75th    int64
		Percentile95th    int64
		Percentile98th    int64
		Percentile99th    int64
		Percentile999th   int64
		MeanRate          float64
		OneMinuteRate     float64
		FiveMinuteRate    float64
		FifteenMinuteRate float64
	}

	Stats struct {
		TotalConnections                 int64
		AvailableConnections             int64
		ExceededPendingRequestsWaterMark int64
		ExceededWriteBytesWaterMark      int64
	}

	Errors struct {
		ConnectionTimeouts     int64
		PendingRequestTimeouts int64
		RequestTimeouts        int64
	}
}

func SetLogLevel(level int32) {
	clevel := C.CASS_LOG_DISABLED
	switch level {
	case CASS_LOG_DISABLED:
		clevel = C.CASS_LOG_DISABLED
	case CASS_LOG_CRITICAL:
		clevel = C.CASS_LOG_CRITICAL
	case CASS_LOG_ERROR:
		clevel = C.CASS_LOG_ERROR
	case CASS_LOG_WARN:
		clevel = C.CASS_LOG_WARN
	case CASS_LOG_INFO:
		clevel = C.CASS_LOG_INFO
	case CASS_LOG_DEBUG:
		clevel = C.CASS_LOG_DEBUG
	case CASS_LOG_TRACE:
		clevel = C.CASS_LOG_TRACE
	}
	C.cass_log_set_level(C.CassLogLevel(clevel))
}

func NewCluster() *Cluster {
	cluster := new(Cluster)
	cluster.cptr = C.cass_cluster_new()
	// defer cluster.Finalize()

	return cluster
}

func NewSession() *Session {
	session := new(Session)
	session.cptr = C.cass_session_new()
	return session
}

func (session *Session) Metrics() Metrics {

	var cmetrics C.CassMetrics
	C.cass_session_get_metrics(session.cptr, &cmetrics)

	var output Metrics
	output.Requests.Min = int64(cmetrics.requests.min)
	output.Requests.Max = int64(cmetrics.requests.max)
	output.Requests.Mean = int64(cmetrics.requests.mean)
	output.Requests.Stddev = int64(cmetrics.requests.stddev)
	output.Requests.Median = int64(cmetrics.requests.median)
	output.Requests.Percentile75th = int64(cmetrics.requests.percentile_75th)
	output.Requests.Percentile95th = int64(cmetrics.requests.percentile_95th)
	output.Requests.Percentile99th = int64(cmetrics.requests.percentile_99th)
	output.Requests.Percentile999th = int64(cmetrics.requests.percentile_999th)
	output.Requests.MeanRate = float64(cmetrics.requests.mean_rate)
	output.Requests.OneMinuteRate = float64(cmetrics.requests.one_minute_rate)
	output.Requests.FiveMinuteRate = float64(cmetrics.requests.five_minute_rate)
	output.Requests.FifteenMinuteRate = float64(cmetrics.requests.fifteen_minute_rate)

	output.Stats.TotalConnections = int64(cmetrics.stats.total_connections)
	output.Stats.AvailableConnections = int64(cmetrics.stats.available_connections)
	output.Stats.ExceededPendingRequestsWaterMark = int64(cmetrics.stats.exceeded_pending_requests_water_mark)
	output.Stats.ExceededWriteBytesWaterMark = int64(cmetrics.stats.exceeded_write_bytes_water_mark)

	output.Errors.ConnectionTimeouts = int64(cmetrics.errors.connection_timeouts)
	output.Errors.PendingRequestTimeouts = int64(cmetrics.errors.pending_request_timeouts)
	output.Errors.RequestTimeouts = int64(cmetrics.errors.request_timeouts)

	return output
}

func NewStatement(query string, param_count int) *Statement {
	cs := C.CString(query)
	defer C.free(unsafe.Pointer(cs))

	statement := new(Statement)
	statement.cptr = C.cass_statement_new(cs, C.size_t(param_count))
	return statement
}

func NewUuidGenerator() *UuidGenerator {
	generator := new(UuidGenerator)
	generator.cptr = C.cass_uuid_gen_new()
	return generator
}

func NewUuidGeneratorWithNode(node uint64) *UuidGenerator {
	generator := new(UuidGenerator)
	generator.cptr = C.cass_uuid_gen_new_with_node(C.cass_uint64_t(node))
	return generator
}

func (generator *UuidGenerator) GenTime() Uuid {
	var uuid Uuid
	C.cass_uuid_gen_time(generator.cptr, &uuid.uuid)
	return uuid
}

func (generator *UuidGenerator) GenRandom() Uuid {
	var uuid Uuid
	C.cass_uuid_gen_random(generator.cptr, &uuid.uuid)
	return uuid
}

func (generator *UuidGenerator) FromTime(timestamp uint64) Uuid {
	var uuid Uuid
	C.cass_uuid_gen_from_time(generator.cptr, C.cass_uint64_t(timestamp), &uuid.uuid)
	return uuid
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
			err = C.cass_statement_bind_null(statement.cptr, C.size_t(i))

		case int32:
			err = C.cass_statement_bind_int32(statement.cptr, C.size_t(i), C.cass_int32_t(v))

		case int64:
			err = C.cass_statement_bind_int64(statement.cptr, C.size_t(i), C.cass_int64_t(v))

		case float32:
			err = C.cass_statement_bind_float(statement.cptr, C.size_t(i), C.cass_float_t(v))

		case float64:
			err = C.cass_statement_bind_double(statement.cptr, C.size_t(i), C.cass_double_t(v))

		case bool:
			if v {
				err = C.cass_statement_bind_bool(statement.cptr, C.size_t(i), 1)
			} else {
				err = C.cass_statement_bind_bool(statement.cptr, C.size_t(i), 0)
			}

		case string:
			err = bind_string(statement, i, v)

		case []byte:
			err = C.cass_statement_bind_bytes(statement.cptr, C.size_t(i), (*C.cass_byte_t)(unsafe.Pointer(&v)), C.size_t(len(v)))

		case Uuid:
			C.cass_statement_bind_uuid(statement.cptr, C.size_t(i), v.uuid)
		}

	}

	if err != C.CASS_OK {
		return errors.New(C.GoString(C.cass_error_desc(err)))
	}

	return nil
}

func bind_string(statement *Statement, index int, v string) C.CassError {
	cs := C.CString(v)
	defer C.free(unsafe.Pointer(cs))
	return C.cass_statement_bind_string(statement.cptr, C.size_t(index), C.CString(v))
}

func (cluster *Cluster) Finalize() {
	C.cass_cluster_free(cluster.cptr)
	cluster.cptr = nil
}

func (session *Session) Finalize() {
	C.cass_session_free(session.cptr)
	session.cptr = nil
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

func (generator *UuidGenerator) Finalize() {
	C.cass_uuid_gen_free(generator.cptr)
	generator.cptr = nil
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

func (future *Future) ErrorMessage() string {
	var message *C.char
	var message_length C.size_t
	C.cass_future_error_message(future.cptr, &message, &message_length)
	return C.GoString(message)
}

func (future *Future) ErrorSource() int {
	rc := C.cass_future_error_code(future.cptr)
	source := (rc >> 24)

	switch source {
	case C.CASS_ERROR_SOURCE_NONE:
		return CASS_OK
	case C.CASS_ERROR_SOURCE_LIB:
		return CASS_ERROR_SOURCE_LIB
	case C.CASS_ERROR_SOURCE_SERVER:
		return CASS_ERROR_SOURCE_SERVER
	case C.CASS_ERROR_SOURCE_SSL:
		return CASS_ERROR_SOURCE_SSL
	case C.CASS_ERROR_SOURCE_COMPRESSION:
		return CASS_ERROR_SOURCE_COMPRESSION
	}
	return CASS_ERROR_SOURCE_NONE
}

func (future *Future) ErrorCode() int {
	rc := C.cass_future_error_code(future.cptr)
	source := future.ErrorSource()

	if source == CASS_ERROR_SOURCE_NONE {
		return CASS_OK
	} else if source == CASS_ERROR_SOURCE_LIB {
		switch rc {
		case CASS_OK:
			return CASS_OK
		case C.CASS_ERROR_LIB_BAD_PARAMS:
			return CASS_ERROR_LIB_BAD_PARAMS
		case C.CASS_ERROR_LIB_NO_STREAMS:
			return CASS_ERROR_LIB_NO_STREAMS
		case C.CASS_ERROR_LIB_UNABLE_TO_INIT:
			return CASS_ERROR_LIB_UNABLE_TO_INIT
		case C.CASS_ERROR_LIB_MESSAGE_ENCODE:
			return CASS_ERROR_LIB_MESSAGE_ENCODE
		case C.CASS_ERROR_LIB_HOST_RESOLUTION:
			return CASS_ERROR_LIB_HOST_RESOLUTION
		case C.CASS_ERROR_LIB_UNEXPECTED_RESPONSE:
			return CASS_ERROR_LIB_UNEXPECTED_RESPONSE
		case C.CASS_ERROR_LIB_REQUEST_QUEUE_FULL:
			return CASS_ERROR_LIB_REQUEST_QUEUE_FULL
		case C.CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD:
			return CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD
		case C.CASS_ERROR_LIB_WRITE_ERROR:
			return CASS_ERROR_LIB_WRITE_ERROR
		case C.CASS_ERROR_LIB_NO_HOSTS_AVAILABLE:
			return CASS_ERROR_LIB_NO_HOSTS_AVAILABLE
		case C.CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS:
			return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS
		case C.CASS_ERROR_LIB_INVALID_ITEM_COUNT:
			return CASS_ERROR_LIB_INVALID_ITEM_COUNT
		case C.CASS_ERROR_LIB_INVALID_VALUE_TYPE:
			return CASS_ERROR_LIB_INVALID_VALUE_TYPE
		case C.CASS_ERROR_LIB_REQUEST_TIMED_OUT:
			return CASS_ERROR_LIB_REQUEST_TIMED_OUT
		case C.CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE:
			return CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE
		case C.CASS_ERROR_LIB_CALLBACK_ALREADY_SET:
			return CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE
		case C.CASS_ERROR_LIB_INVALID_STATEMENT_TYPE:
			return CASS_ERROR_LIB_INVALID_STATEMENT_TYPE
		case C.CASS_ERROR_LIB_NAME_DOES_NOT_EXIST:
			return CASS_ERROR_LIB_NAME_DOES_NOT_EXIST
		case C.CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL:
			return CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL
		case C.CASS_ERROR_LIB_NULL_VALUE:
			return CASS_ERROR_LIB_NULL_VALUE
		case C.CASS_ERROR_LIB_NOT_IMPLEMENTED:
			return CASS_ERROR_LIB_NOT_IMPLEMENTED
		case C.CASS_ERROR_LIB_UNABLE_TO_CONNECT:
			return CASS_ERROR_LIB_UNABLE_TO_CONNECT
		case C.CASS_ERROR_LIB_UNABLE_TO_CLOSE:
			return CASS_ERROR_LIB_UNABLE_TO_CLOSE
		}
	} else if source == CASS_ERROR_SOURCE_SERVER {
		switch rc {
		case C.CASS_OK:
			return C.CASS_OK
		case C.CASS_ERROR_SERVER_SERVER_ERROR:
			return CASS_ERROR_SERVER_SERVER_ERROR
		case C.CASS_ERROR_SERVER_PROTOCOL_ERROR:
			return CASS_ERROR_SERVER_PROTOCOL_ERROR
		case C.CASS_ERROR_SERVER_BAD_CREDENTIALS:
			return CASS_ERROR_SERVER_BAD_CREDENTIALS
		case C.CASS_ERROR_SERVER_UNAVAILABLE:
			return CASS_ERROR_SERVER_UNAVAILABLE
		case C.CASS_ERROR_SERVER_OVERLOADED:
			return CASS_ERROR_SERVER_OVERLOADED
		case C.CASS_ERROR_SERVER_IS_BOOTSTRAPPING:
			return CASS_ERROR_SERVER_IS_BOOTSTRAPPING
		case C.CASS_ERROR_SERVER_TRUNCATE_ERROR:
			return CASS_ERROR_SERVER_TRUNCATE_ERROR
		case C.CASS_ERROR_SERVER_WRITE_TIMEOUT:
			return CASS_ERROR_SERVER_WRITE_TIMEOUT
		case C.CASS_ERROR_SERVER_READ_TIMEOUT:
			return CASS_ERROR_SERVER_READ_TIMEOUT
		case C.CASS_ERROR_SERVER_SYNTAX_ERROR:
			return CASS_ERROR_SERVER_SYNTAX_ERROR
		case C.CASS_ERROR_SERVER_UNAUTHORIZED:
			return CASS_ERROR_SERVER_UNAUTHORIZED
		case C.CASS_ERROR_SERVER_INVALID_QUERY:
			return CASS_ERROR_SERVER_INVALID_QUERY
		case C.CASS_ERROR_SERVER_CONFIG_ERROR:
			return CASS_ERROR_SERVER_CONFIG_ERROR
		case C.CASS_ERROR_SERVER_ALREADY_EXISTS:
			return CASS_ERROR_SERVER_ALREADY_EXISTS
		case C.CASS_ERROR_SERVER_UNPREPARED:
			return CASS_ERROR_SERVER_UNPREPARED
		}
	} else if source == CASS_ERROR_SOURCE_SSL {
		switch rc {
		case CASS_OK:
			return C.CASS_OK
		case C.CASS_ERROR_SSL_INVALID_CERT:
			return CASS_ERROR_SSL_INVALID_CERT
		case C.CASS_ERROR_SSL_INVALID_PRIVATE_KEY:
			return CASS_ERROR_SSL_INVALID_PRIVATE_KEY
		case C.CASS_ERROR_SSL_NO_PEER_CERT:
			return CASS_ERROR_SSL_NO_PEER_CERT
		case C.CASS_ERROR_SSL_INVALID_PEER_CERT:
			return CASS_ERROR_SSL_INVALID_PEER_CERT
		case C.CASS_ERROR_SSL_IDENTITY_MISMATCH:
			return CASS_ERROR_SSL_IDENTITY_MISMATCH
		}
	}
	return CASS_ERROR_LIB_UNEXPECTED_RESPONSE
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

func (cluster *Cluster) SetNumThreadsIo(size uint) {
	C.cass_cluster_set_num_threads_io(cluster.cptr, C.uint(size))
}

func (cluster *Cluster) SetQueueSizeIo(size uint) {
	C.cass_cluster_set_queue_size_io(cluster.cptr, C.uint(size))
}

func (cluster *Cluster) SetPendingRequestsLowWaterMark(size uint) {
	C.cass_cluster_set_pending_requests_low_water_mark(cluster.cptr, C.uint(size))
}

func (cluster *Cluster) SetPendingRequestsHighWaterMark(size uint) {
	C.cass_cluster_set_pending_requests_high_water_mark(cluster.cptr, C.uint(size))
}

func (cluster *Cluster) SetCoreConnectionsPerHost(size uint) {
	C.cass_cluster_set_core_connections_per_host(cluster.cptr, C.uint(size))
}

func (cluster *Cluster) SetMaxConnectionsPerHost(size uint) {
	C.cass_cluster_set_max_connections_per_host(cluster.cptr, C.uint(size))
}

func (cluster *Cluster) SessionConnect(session *Session) *Future {
	future := new(Future)
	future.cptr = C.cass_session_connect(session.cptr, cluster.cptr)
	return future
}

func (session *Session) Execute(statement *Statement) *Future {
	future := new(Future)
	future.cptr = C.cass_session_execute(session.cptr, statement.cptr)
	return future
}

func (session *Session) Prepare(statement string) *Future {
	cstring := C.CString(statement)
	future := new(Future)
	future.cptr = C.cass_session_prepare(session.cptr, cstring)
	return future
}

func (result *Result) RowCount() uint64 {
	return uint64(C.cass_result_row_count(result.cptr))
}

func (result *Result) ColumnCount() uint64 {
	return uint64(C.cass_result_column_count(result.cptr))
}

// func (result *Result) ColumnName(index uint64) string {
// 	column_name := C.cass_result_column_name(result.cptr, C.size_t(index))
// 	return C.GoStringN(column_name.data, C.int(column_name.length))
// }

func (result *Result) ColumnType(index uint64) int {
	return int(C.cass_result_column_type(result.cptr, C.size_t(index)))
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
		value := C.cass_row_get_column(row, C.size_t(i))

		switch v := v.(type) {

		case *string:
			// var str unsafe.Pointer
			// var length int64
			// err = C.cass_value_get_string(value, &str, &length)
			// if err != C.CASS_OK {
			// 	return errors.New(C.GoString(C.cass_error_desc(err)))
			// }
			// *v = C.GoStringN(str, length)

		case *[]byte:
			// var b *[]byte
			// var l int64
			// err = C.cass_value_get_bytes(value, &b, &l)
			// if err != C.CASS_OK {
			// 	return errors.New(C.GoString(C.cass_error_desc(err)))
			// }
			// *v = C.GoBytes(unsafe.Pointer(b), l)

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
