package pgwire

// Postgres wire protocol message types
// Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html

// Frontend (client -> server) message types
const (
	// Startup
	MsgStartup       byte = 0 // Special - no type byte, identified by length
	MsgSSLRequest    byte = 0 // Special - identified by specific payload
	MsgGSSENCRequest byte = 0 // Special - identified by specific payload
	MsgCancel        byte = 0 // Special - cancel request

	// Simple query
	MsgQuery byte = 'Q'

	// Extended query
	MsgParse    byte = 'P'
	MsgBind     byte = 'B'
	MsgDescribe byte = 'D'
	MsgExecute  byte = 'E'
	MsgClose    byte = 'C'
	MsgSync     byte = 'S'
	MsgFlush    byte = 'H'

	// Other
	MsgTerminate    byte = 'X'
	MsgCopyData     byte = 'd'
	MsgCopyDone     byte = 'c'
	MsgCopyFail     byte = 'f'
	MsgPassword     byte = 'p'
	MsgFunctionCall byte = 'F'
)

// Backend (server -> client) message types
const (
	MsgAuthentication       byte = 'R'
	MsgBackendKeyData       byte = 'K'
	MsgBindComplete         byte = '2'
	MsgCloseComplete        byte = '3'
	MsgCommandComplete      byte = 'C'
	MsgCopyInResponse       byte = 'G'
	MsgCopyOutResponse      byte = 'H'
	MsgCopyBothResponse     byte = 'W'
	MsgDataRow              byte = 'D'
	MsgEmptyQueryResponse   byte = 'I'
	MsgErrorResponse        byte = 'E'
	MsgNoData               byte = 'n'
	MsgNoticeResponse       byte = 'N'
	MsgNotificationResponse byte = 'A'
	MsgParameterDescription byte = 't'
	MsgParameterStatus      byte = 'S'
	MsgParseComplete        byte = '1'
	MsgPortalSuspended      byte = 's'
	MsgReadyForQuery        byte = 'Z'
	MsgRowDescription       byte = 'T'
)

// Authentication types
const (
	AuthOK                = 0
	AuthKerberosV5        = 2
	AuthCleartextPassword = 3
	AuthMD5Password       = 5
	AuthSCMCredential     = 6
	AuthGSS               = 7
	AuthGSSContinue       = 8
	AuthSSPI              = 9
	AuthSASL              = 10
	AuthSASLContinue      = 11
	AuthSASLFinal         = 12
)

// Transaction status indicators (ReadyForQuery)
const (
	TxStatusIdle   byte = 'I' // Not in a transaction
	TxStatusInTx   byte = 'T' // In a transaction
	TxStatusFailed byte = 'E' // In a failed transaction
)

// Protocol version
const (
	ProtocolVersionNumber = 196608 // 3.0 = (3 << 16) | 0
	SSLRequestCode        = 80877103
	CancelRequestCode     = 80877102
	GSSENCRequestCode     = 80877104
)

// Error and notice field types
const (
	FieldSeverity         byte = 'S'
	FieldSeverityNonLocal byte = 'V'
	FieldCode             byte = 'C'
	FieldMessage          byte = 'M'
	FieldDetail           byte = 'D'
	FieldHint             byte = 'H'
	FieldPosition         byte = 'P'
	FieldInternalPosition byte = 'p'
	FieldInternalQuery    byte = 'q'
	FieldWhere            byte = 'W'
	FieldSchema           byte = 's'
	FieldTable            byte = 't'
	FieldColumn           byte = 'c'
	FieldDataType         byte = 'd'
	FieldConstraint       byte = 'n'
	FieldFile             byte = 'F'
	FieldLine             byte = 'L'
	FieldRoutine          byte = 'R'
)

// Common error codes
const (
	ErrCodeSuccessfulCompletion  = "00000"
	ErrCodeWarning               = "01000"
	ErrCodeNoData                = "02000"
	ErrCodeConnectionException   = "08000"
	ErrCodeConnectionFailure     = "08006"
	ErrCodeSyntaxError           = "42601"
	ErrCodeInvalidCatalogName    = "3D000"
	ErrCodeUndefinedTable        = "42P01"
	ErrCodeInsufficientPrivilege = "42501"
	ErrCodeInternalError         = "XX000"
)
