package cassandra
// #cgo LDFLAGS: -lcassandra
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import "unsafe"

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

type CassCluster struct {
	cptr *C.struct_CassCluster_
}

type CassFuture struct {
	cptr *C.struct_CassFuture_
}

type CassSession struct {
	cptr *C.struct_CassSession_
}

type CassResult struct {
	cptr *C.struct_CassResult_
}

type CassPrepared struct {
	cptr *C.struct_CassPrepared_
}

type CassStatement struct {
	cptr *C.struct_CassStatement_
}

func NewCassCluster()(*CassCluster) {
	cluster := new(CassCluster)
	cluster.cptr = C.cass_cluster_new()
	defer cluster.Finalize()

	return cluster
}

func NewCassStatement(query *string, param_count int)(*CassStatement) {
	var cass_query C.struct_CassString_
	cass_query.data = C.CString(*query)
	cass_query.length = C.cass_size_t(len(*query))
	defer C.free(unsafe.Pointer(cass_query.data))

	statement := new(CassStatement)
	statement.cptr = C.cass_statement_new(cass_query, C.cass_size_t(param_count))
	defer statement.Finalize()

	return statement
}

func (cluster *CassCluster) Finalize() {
	C.cass_cluster_free(cluster.cptr)
	cluster.cptr = nil
}

func (future *CassFuture) Finalize() {
	C.cass_future_free(future.cptr)
	future.cptr = nil
}

func (result *CassResult) Finalize() {
	C.cass_result_free(result.cptr)
	result.cptr = nil
}

func (prepared *CassPrepared) Finalize() {
	C.cass_prepared_free(prepared.cptr)
	prepared.cptr = nil
}

func (statement *CassStatement) Finalize() {
	C.cass_statement_free(statement.cptr)
	statement.cptr = nil
}

func (future *CassFuture) Session() *CassSession {
	session := new(CassSession)
	session.cptr = C.cass_future_get_session(future.cptr)
	return session
}

func (future *CassFuture) Result() *CassResult {
	result := new(CassResult)
	result.cptr = C.cass_future_get_result(future.cptr)
	defer result.Finalize()

	return result
}

func (future *CassFuture) Prepared() *CassPrepared {
	prepared := new(CassPrepared)
	prepared.cptr = C.cass_future_get_prepared(future.cptr)
	defer prepared.Finalize()

	return prepared
}

func (future *CassFuture) Ready() bool {
	return C.cass_future_ready(future.cptr) == C.cass_true
}

func (future *CassFuture) Wait() {
	C.cass_future_wait(future.cptr)
}

func (future *CassFuture) WaitTimed(timeout uint64) bool {
	return C.cass_future_wait_timed(future.cptr, C.cass_duration_t(timeout)) == C.cass_true
}

func (cluster *CassCluster) SetContactPoints(contactPoints *string) {
	contacts_cstr := C.CString(*contactPoints)
	defer C.free(unsafe.Pointer(contacts_cstr))

	C.cass_cluster_set_contact_points(cluster.cptr, contacts_cstr)
}

func (cluster *CassCluster) SetPort(port int64){
	port_cint := C.int(port)
	C.cass_cluster_set_port(cluster.cptr, port_cint)
}

func (cluster *CassCluster) Connect() *CassFuture {
	future := new(CassFuture)
	future.cptr = C.cass_cluster_connect(cluster.cptr)
	defer future.Finalize()

	return future
}

func (cluster *CassCluster) ConnectKeyspace (keyspace string) *CassFuture {
	keyspace_cstr := C.CString(keyspace)
	defer C.free(unsafe.Pointer(keyspace_cstr))

	future := new(CassFuture)
	future.cptr = C.cass_cluster_connect_keyspace(cluster.cptr, keyspace_cstr)
	defer future.Finalize()

	return future
}

func (session *CassSession) Execute(statement *CassStatement) *CassFuture {
	future := new(CassFuture)
	future.cptr = C.cass_session_execute(session.cptr, statement.cptr)
	return future
}

func (result *CassResult) RowCount() uint64 {
	return uint64(C.cass_result_row_count(result.cptr))
}

func (result *CassResult) ColumnCount() uint64 {
	return uint64(C.cass_result_column_count(result.cptr))
}

func (result *CassResult) ColumnName(index uint64) string {
	column_name := C.cass_result_column_name(result.cptr, C.cass_size_t(index))
	return C.GoStringN(column_name.data, C.int(column_name.length))
}

func (result *CassResult) ColumnType(index uint64) int {
	return int(C.cass_result_column_type(result.cptr, C.cass_size_t(index)))
}

func (result *CassResult) HasMorePages() bool {
	return C.cass_result_has_more_pages(result.cptr) != 0
}

func (result *CassResult) Next() bool {
	return C.cass_result_has_more_pages(result.cptr) != 0
}
