package utils

import (
	"client-go-iotdb/gen-go/rpc"
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
)

type Session struct {
	Host            string
	Port            string
	User            string
	Password        string
	FetchSize       int64
	IsClose         bool
	Transport       thrift.TTransport
	Client          *rpc.TSIServiceClient
	ProtocolVersion rpc.TSProtocolVersion
	SessionId       int64
	StatumentId     int64
	ZoneId          string
}

var default_Ctx = context.Background()
var default_UserName = "root"
var default_Passwd = "root"
var default_Host = "192.168.5.171"
var default_Port = "6667"
var default_ZoneId = "UTC+8"
var default_SuccessCode = 200
var default_FetchSize int64 = 10000

func NewSession() *Session {
	return &Session{Host: default_Host, Port: default_Port, ZoneId: default_ZoneId, User: default_UserName, Password: default_Passwd, FetchSize: default_FetchSize, IsClose: true, ProtocolVersion: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3}
}

func (s_ *Session) Is_Open() bool {
	return !s_.IsClose
}

func (s_ *Session) Close(enable_rpc_compression bool) {
	if s_.IsClose {
		return
	}
	defer s_.Transport.Close()
	req := &rpc.TSCloseSessionReq{s_.SessionId}
	s_.Client.CloseSession(default_Ctx, req)
	s_.IsClose = true
}

func (s_ *Session) Open(enable_rpc_compression bool) error {
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	tSocket, err := thrift.NewTSocket(s_.Host + ":" + s_.Port)
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return err
	}
	transport, err := transportFactory.GetTransport(tSocket)
	if err != nil {
		return err
	}
	if err := transport.Open(); err != nil {
		return err
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	client := rpc.NewTSIServiceClient(thrift.NewTStandardClient(iprot, oprot))
	req := rpc.NewTSOpenSessionReq()
	req.ClientProtocol = s_.ProtocolVersion
	req.Username = &s_.User
	req.Password = &s_.Password
	req.ZoneId = s_.ZoneId
	rsp, err := client.OpenSession(default_Ctx, req)
	if err == nil {
		fmt.Printf("Open:::", rsp)
		s_.SessionId = *rsp.SessionId
		s_.StatumentId, _ = s_.Client.RequestStatementId(default_Ctx, s_.SessionId)
		s_.IsClose = false
	} else {
		fmt.Printf("OpenError:::", err)
	}
	return err
}
