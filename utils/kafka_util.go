package utils

import "github.com/protocol-laboratory/kafka-codec-go/codec"

func ConvertMap2Headers(m map[string]string) []*codec.Header {
	headers := make([]*codec.Header, 0)
	for k, v := range m {
		headers = append(headers, &codec.Header{Key: k, Value: []byte(v)})
	}
	return headers
}
