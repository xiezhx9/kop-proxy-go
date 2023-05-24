//go:build integration

package kop

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
)

func TestIntegrateKopTcpConnect(t *testing.T) {
	kop, port := testSetupKop()
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	require.NotNil(t, conn)
	time.Sleep(time.Second)
	require.Equal(t, int32(1), kop.getConnCount())
	_ = conn.Close()
	time.Sleep(time.Second)
	require.Equal(t, int32(0), kop.getConnCount())
	kop.Close()
}
