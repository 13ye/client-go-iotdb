package main

import (
	"client-go-iotdb/gen-go/rpc"
	"context"
	"crypto/tls"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

var defaultCtx = context.Background()
var userName = "root"
var passwd = "root"

func handleClient(client *rpc.TSIServiceClient) (err error) {
	req := rpc.NewTSOpenSessionReq()
	req.ClientProtocol = rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3
	req.Username = &userName
	req.Password = &passwd
	req.ZoneId = "UTC+8"
	rsp, err := client.OpenSession(defaultCtx, req)
	fmt.Println("HandleClient:::", rsp, err)

	return err
}

func runClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool) error {
	var transport thrift.TTransport
	var err error
	if secure {
		cfg := new(tls.Config)
		cfg.InsecureSkipVerify = true
		transport, err = thrift.NewTSSLSocket(addr, cfg)
	} else {
		transport, err = thrift.NewTSocket(addr)
	}
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return err
	}
	transport, err = transportFactory.GetTransport(transport)
	if err != nil {
		return err
	}
	defer transport.Close()
	if err := transport.Open(); err != nil {
		return err
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	return handleClient(rpc.NewTSIServiceClient(thrift.NewTStandardClient(iprot, oprot)))
}

func main() {
	transportFactory := thrift.NewTTransportFactory()
	transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	err := runClient(transportFactory, protocolFactory, "192.168.5.171:6667", false)
	if err != nil {
		fmt.Println(err.Error())
	}
}
