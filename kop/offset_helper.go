package kop

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"strconv"
)

func convOffset(message pulsar.Message, continuousOffset bool) int64 {
	if continuousOffset {
		index := message.Index()
		if index == nil {
			panic(any("continuous offset mode, index field must be set"))
		}
		return int64(*index) + 1
	}
	return ConvertMsgId(message.ID())
}

func ConvertMsgId(messageId pulsar.MessageID) int64 {
	offset, _ := strconv.Atoi(fmt.Sprint(messageId.LedgerID()) + fmt.Sprint(messageId.EntryID()) + fmt.Sprint(messageId.PartitionIdx()))
	return int64(offset)
}
