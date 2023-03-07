package model

type MessageID struct {
	LedgerID     int64
	EntryID      int64
	BatchIdx     int32
	PartitionIdx int32
}

type MessageIdData struct {
	MessageId []byte
	Offset    int64
}
