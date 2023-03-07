package constant

import "time"

const (
	DefaultOffset = int64(0)
	UnknownOffset = int64(-1)

	TimeEarliest = int64(-2)
	TimeLasted   = int64(-1)

	OffsetReaderEarliestName = "OFFSET_LIST_EARLIEST"

	DefaultProducerSendTimeout = 1 * time.Second
	DefaultMaxPendingMsg       = 100

	PartitionSuffixFormat = "-partition-%d"
)

const (
	LastMsgIdUrl = "/admin/v2/persistent/%s/%s/%s/lastMessageId"
)

const (
	ReadMsgTimeoutErr = "context deadline exceeded"
)
