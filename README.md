# kop-proxy-go

kop-proxy-go (Kafka on Pulsar implement by golang) brings the native Apache Kafka protocol support to Apache Pulsar by introducing a Kafka protocol handler on Pulsar brokers. It uses golang to implement the complete Kafka client and server protocol.

## How to use kop-proxy-go

1. define a structure to implement the Server interface
```go
type ItKopImpl struct {}
var _ kop.Server = (*ItKopImpl)(nil)

func (e ItKopImpl) Auth(username string, password string, clientId string) (bool, error) {
    return true, nil
}
// ... other method implement
```

2. run a kop instance
```go
func main() {
    config := &kop.Config{}
    e := &ItKopImpl{}
    impl, err := kop.NewKop(e, config)
    if err != nil {
        panic(err)
    }
    err = impl.Run()
    if err != nil {
        panic(err)
    }
    interrupt := make(chan os.Signal, 1)
    signal.Notify(interrupt, os.Interrupt)
    for range interrupt {
        impl.Close()
        return
    }
}
```

use case: [kop-proxy-go/cmd/it](https://github.com/protocol-laboratory/kop-proxy-go/tree/main/cmd/it)
