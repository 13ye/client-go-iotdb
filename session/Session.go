package session

import (
	"bytes"
	"client-go-iotdb/gen-go/rpc"
	"client-go-iotdb/utils"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
)

type Session struct {
	Host            string
	Port            string
	User            string
	Password        string
	FetchSize       int32
	SuccessCode     int64
	IsClose         bool
	Transport       thrift.TTransport
	Client          *rpc.TSIServiceClient
	ProtocolVersion rpc.TSProtocolVersion
	SessionId       int64
	StatementId     int64
	ZoneId          string
}

var default_Ctx = context.Background()
var default_UserName = "root"
var default_Passwd = "root"
var default_Host = "192.168.5.171"
var default_Port = "6667"
var default_ZoneId = "UTC+8"
var default_SuccessCode int64 = 200
var default_FetchSize int32 = 10000

func NewSession() *Session {
	return &Session{Host: default_Host, Port: default_Port, ZoneId: default_ZoneId, User: default_UserName, Password: default_Passwd, SuccessCode: default_SuccessCode, FetchSize: default_FetchSize, IsClose: true, ProtocolVersion: rpc.TSProtocolVersion_IOTDB_SERVICE_PROTOCOL_V3}
}

func (s_ *Session) Is_Open() bool {
	return !s_.IsClose
}

func (s_ *Session) Close(enable_rpc_compression bool) {
	if s_.IsClose {
		return
	}
	defer s_.Transport.Close()
	req := &rpc.TSCloseSessionReq{SessionId: s_.SessionId}
	s_.Client.CloseSession(default_Ctx, req)
	s_.IsClose = true
}

func (s_ *Session) Open(enable_rpc_compression bool) {
	if s_.Is_Open() {
		return
	}
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	var protocolFactory thrift.TProtocolFactory
	if enable_rpc_compression {
		protocolFactory = thrift.NewTCompactProtocolFactory()
	} else {
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}
	tSocket, err := thrift.NewTSocket(s_.Host + ":" + s_.Port)
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return
	}
	transport, err := transportFactory.GetTransport(tSocket)
	if err != nil {
		fmt.Println("Error getting Transport:", err)
		return
	}
	if err := transport.Open(); err != nil {
		fmt.Println("Error opening Transport:", err)
		return
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)

	s_.Client = rpc.NewTSIServiceClient(thrift.NewTStandardClient(iprot, oprot))
	req := rpc.NewTSOpenSessionReq()
	req.ClientProtocol = s_.ProtocolVersion
	req.Username = &s_.User
	req.Password = &s_.Password
	req.ZoneId = s_.ZoneId
	rsp, err := s_.Client.OpenSession(default_Ctx, req)
	if err == nil {
		if rsp.GetServerProtocolVersion() != s_.ProtocolVersion {
			fmt.Printf("Error ProtocolVersion Differ, Client Version{%v}, Server Version{%v}\n", s_.ProtocolVersion, rsp.GetServerProtocolVersion())
			return
		}
		fmt.Printf("OpenRsp:::%v\n", rsp)
		s_.SessionId = *rsp.SessionId
		s_.StatementId, _ = s_.Client.RequestStatementId(default_Ctx, s_.SessionId)
		s_.IsClose = false
	} else {
		fmt.Println("Error OpenRequest:", err, rsp)
		return
	}
}

func (s_ *Session) SetStorageGroup(groupName string) bool {
	status, _ := s_.Client.SetStorageGroup(default_Ctx, s_.SessionId, groupName)
	fmt.Printf("Setting storage group {%v} message: {%v}\n", groupName, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) DeleteStorageGroup(storageGroup string) bool {
	return s_.DeleteStorageGroups([]string{storageGroup})
}

func (s_ *Session) DeleteStorageGroups(storageGroups []string) bool {
	status, _ := s_.Client.DeleteStorageGroups(default_Ctx, s_.SessionId, storageGroups)
	fmt.Printf("Delete storage group {%v} message: {%v}\n", storageGroups, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) CreateTimeSeries(tsPath string, dataType int32, encoding int32, compressor int32) bool {
	request := &rpc.TSCreateTimeseriesReq{SessionId: s_.SessionId, Path: tsPath, DataType: dataType, Encoding: encoding, Compressor: compressor}
	status, _ := s_.Client.CreateTimeseries(default_Ctx, request)
	fmt.Printf("Creating time series {%v} message: {%v}\n", tsPath, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) CreateMultiTimeSeries(tsPaths []string, dataTypes []int32, encodings []int32, compressors []int32) bool {
	request := &rpc.TSCreateMultiTimeseriesReq{SessionId: s_.SessionId, Paths: tsPaths, DataTypes: dataTypes, Encodings: encodings, Compressors: compressors}
	status, _ := s_.Client.CreateMultiTimeseries(default_Ctx, request)
	fmt.Printf("Creating multiple time series {%v} message: {%v}\n", tsPaths, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) DeleteTimeSeries(paths []string) bool {
	status, _ := s_.Client.DeleteTimeseries(default_Ctx, s_.SessionId, paths)
	fmt.Printf("Delete multiple time series {%v} message: {%v}\n", paths, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) DeleteData(paths []string, startTime int64, endTime int64) bool {
	request := &rpc.TSDeleteDataReq{SessionId: s_.SessionId, Paths: paths, StartTime: startTime, EndTime: endTime}
	status, _ := s_.Client.DeleteData(default_Ctx, request)
	fmt.Printf("Delete data from{%v} message: {%v}\n", paths, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) InsertStrRecord(deviceId string, measurements []string, values_str []string, timestamp int64) bool {
	dataTypes := make([]int32, len(values_str))
	values := make([]interface{}, len(values_str))
	for k, v := range values_str {
		dataTypes[k] = utils.TSDataType.TEXT
		values[k] = v
	}
	request := s_.GenInsertRecordReq(deviceId, measurements, values, dataTypes, timestamp)
	status, _ := s_.Client.InsertRecord(default_Ctx, request)
	fmt.Printf("Insert One Record to device: {%v} message: {%v}\n", deviceId, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) InsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{}, timestamp int64) bool {
	request := s_.GenInsertRecordReq(deviceId, measurements, values, dataTypes, timestamp)
	if request == nil {
		fmt.Println("GenInsertTabletReq Failed!")
		return false
	}
	status, _ := s_.Client.InsertRecord(default_Ctx, request)
	fmt.Printf("Insert One Record to device: {%v} message: {%v}\n", deviceId, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) InsertRecords(deviceIds []string, measurements_list [][]string, dataTypes_list [][]int32, values_list [][]interface{}, timestamps []int64) bool {
	request := s_.GenInsertRecordsReq(deviceIds, measurements_list, values_list, dataTypes_list, timestamps)
	if request == nil {
		fmt.Println("GenInsertTabletReq Failed!")
		return false
	}
	status, _ := s_.Client.InsertRecords(default_Ctx, request)
	fmt.Printf("Insert Multiple Records to device: {%v} message: {%v}\n", deviceIds, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) InsertTablet(tablet utils.Tablet) bool {
	request := s_.GenInsertTabletReq(tablet)
	if request == nil {
		fmt.Println("GenInsertTabletReq Failed!")
		return false
	}
	status, _ := s_.Client.InsertTablet(default_Ctx, request)
	fmt.Printf("Insert One Tablet to device: {%v} message: {%v}\n", tablet.GetDeviceId(), status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) InsertTablets(tablets []utils.Tablet) bool {
	request := s_.GenInsertTabletsReq(tablets)
	if request == nil {
		fmt.Println("GenInsertTabletsReq Failed!")
		return false
	}
	status, _ := s_.Client.InsertTablets(default_Ctx, request)
	fmt.Printf("Insert Multiple Tablets, message: {%v}\n", status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) TestInsertRecord(deviceId string, measurements []string, dataTypes []int32, values []interface{}, timestamp int64) bool {
	request := s_.GenInsertRecordReq(deviceId, measurements, values, dataTypes, timestamp)
	status, _ := s_.Client.TestInsertRecord(default_Ctx, request)
	fmt.Printf("Test Insert One Record to device: {%v} message: {%v}\n", deviceId, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) TestInsertRecords(deviceIds []string, measurements_list [][]string, dataTypes_list [][]int32, values_list [][]interface{}, timestamps []int64) bool {
	request := s_.GenInsertRecordsReq(deviceIds, measurements_list, values_list, dataTypes_list, timestamps)
	status, _ := s_.Client.TestInsertRecords(default_Ctx, request)
	fmt.Printf("Test Insert Multiple Records to device: {%v} message: {%v}\n", deviceIds, status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) TestInsertTablet(tablet utils.Tablet) bool {
	request := s_.GenInsertTabletReq(tablet)
	if request == nil {
		fmt.Println("GenInsertTabletReq Failed!")
		return false
	}
	status, _ := s_.Client.TestInsertTablet(default_Ctx, request)
	fmt.Printf("Test Insert One Tablet to device: {%v} message: {%v}\n", tablet.GetDeviceId(), status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) TestInsertTablets(tablets []utils.Tablet) bool {
	request := s_.GenInsertTabletsReq(tablets)
	if request == nil {
		fmt.Println("GenInsertTabletsReq Failed!")
		return false
	}
	status, _ := s_.Client.TestInsertTablets(default_Ctx, request)
	fmt.Printf("Test Insert Multiple Tablets, message: {%v}\n", status.GetMessage())
	return s_.verifySuccess(int64(status.GetCode()))
}

func (s_ *Session) GenInsertRecordReq(deviceId string, measurements []string, values []interface{}, dataTypes []int32, timestamp int64) *rpc.TSInsertRecordReq {
	if len(values) != len(dataTypes) || len(values) != len(measurements) {
		fmt.Println("Slices Length Don't Match! {GenInsertRecordReq}")
		return nil
	}
	values_in_bytes := s_.value2Bytes(dataTypes, values)
	request := &rpc.TSInsertRecordReq{SessionId: s_.SessionId, DeviceId: deviceId, Measurements: measurements, Values: values_in_bytes, Timestamp: timestamp}
	return request
}

func (s_ *Session) GenInsertRecordsReq(deviceIds []string, measurements_list [][]string, values_list [][]interface{}, dataTypes_list [][]int32, timestamps []int64) *rpc.TSInsertRecordsReq {
	if len(deviceIds) != len(measurements_list) || len(measurements_list) != len(values_list) || len(values_list) != len(dataTypes_list) || len(dataTypes_list) != len(timestamps) {
		fmt.Println("Slices Length Don't Match! {GenInsertRecordsReq}")
		return nil
	}
	values_in_bytes_list := make([][]byte, len(values_list))
	for k, v := range values_list {
		values_in_bytes_list[k] = s_.value2Bytes(dataTypes_list[k], v)
	}
	request := &rpc.TSInsertRecordsReq{SessionId: s_.SessionId, DeviceIds: deviceIds, MeasurementsList: measurements_list, ValuesList: values_in_bytes_list, Timestamps: timestamps}
	return request
}

func (s_ *Session) GenInsertTabletReq(tablet utils.Tablet) *rpc.TSInsertTabletReq {
	request := &rpc.TSInsertTabletReq{SessionId: s_.SessionId, DeviceId: tablet.GetDeviceId(), Measurements: tablet.GetMeasurements(), Values: tablet.GetValuesBinary(), Timestamps: tablet.GetTimestampsBinary(), Types: tablet.GetDataTypes(), Size: int32(tablet.GetRowNumber())}
	return request
}

func (s_ *Session) GenInsertTabletsReq(tablet_list []utils.Tablet) *rpc.TSInsertTabletsReq {
	deviceIds := make([]string, len(tablet_list))
	measurements_list := make([][]string, len(tablet_list))
	values_list := make([][]byte, len(tablet_list))
	timestamps_list := make([][]byte, len(tablet_list))
	dataTypes_list := make([][]int32, len(tablet_list))
	size_list := make([]int32, len(tablet_list))
	for k, v := range tablet_list {
		deviceIds[k] = v.GetDeviceId()
		measurements_list[k] = v.GetMeasurements()
		values_list[k] = v.GetValuesBinary()
		timestamps_list[k] = v.GetTimestampsBinary()
		dataTypes_list[k] = v.GetDataTypes()
		size_list[k] = int32(v.GetRowNumber())
	}
	request := &rpc.TSInsertTabletsReq{SessionId: s_.SessionId, DeviceIds: deviceIds, MeasurementsList: measurements_list, ValuesList: values_list, TimestampsList: timestamps_list, TypesList: dataTypes_list, SizeList: size_list}
	return request
}

func (s_ *Session) CheckTimeSeriesExists(path string) bool {
	dataset := s_.ExecuteQueryStatement(fmt.Sprintf("SHOW TIMESERIES %v", path))
	rlt := dataset.HasNext()
	dataset.CloseOperationHandle()
	return rlt
}

func (s_ *Session) ExecuteQueryStatement(sql string) *utils.SessionDataSet {
	request := &rpc.TSExecuteStatementReq{SessionId: s_.SessionId, Statement: sql, StatementId: s_.StatementId, FetchSize: &s_.FetchSize}
	response, _ := s_.Client.ExecuteQueryStatement(default_Ctx, request)
	return utils.NewSessionDataSet(sql, response.Columns, *utils.GetTSDataTypeFromStringList(response.DataTypeList), response.ColumnNameIndexMap, *response.QueryId, s_.Client, s_.SessionId, response.QueryDataSet, *response.IgnoreTimeStamp)
}

func (s_ *Session) ExecuteNonQueryStatement(sql string) bool {
	request := &rpc.TSExecuteStatementReq{SessionId: s_.SessionId, Statement: sql, StatementId: s_.StatementId}
	response, _ := s_.Client.ExecuteUpdateStatement(default_Ctx, request)
	fmt.Printf("ExecuteNonQueryStatement {%v} message: {%v}\n", sql, response.GetStatus().GetMessage())
	return s_.verifySuccess(int64(response.GetStatus().GetCode()))
}

func (s_ *Session) value2Bytes(dataTypes []int32, values []interface{}) []byte {
	buf := new(bytes.Buffer)
	for k, v := range values {
		if dataTypes[k] == utils.TSDataType.TEXT {
			v_str, ok := v.(string)
			if !ok {
				fmt.Println("value is not type string")
				return nil
			}
			v_bytes := []byte(v_str)
			err1 := binary.Write(buf, binary.BigEndian, byte(dataTypes[k]))
			if err1 != nil {
				fmt.Println("binary.Write failed:", err1)
				return nil
			}
			err2 := binary.Write(buf, binary.BigEndian, len(v_bytes))
			if err2 != nil {
				fmt.Println("binary.Write failed:", err2)
				return nil
			}
			err3 := binary.Write(buf, binary.BigEndian, v_bytes)
			if err3 != nil {
				fmt.Println("binary.Write failed:", err3)
				return nil
			}
		} else {
			err1 := binary.Write(buf, binary.BigEndian, byte(dataTypes[k]))
			if err1 != nil {
				fmt.Println("binary.Write failed:", err1)
				return nil
			}
			err2 := binary.Write(buf, binary.BigEndian, v)
			if err2 != nil {
				fmt.Println("binary.Write failed:", err2)
				return nil
			}
		}
	}
	return buf.Bytes()
}

func (s_ *Session) GetTimeZone() string {
	if s_.ZoneId != "" {
		return s_.ZoneId
	}
	response, _ := s_.Client.GetTimeZone(default_Ctx, s_.SessionId)
	return response.GetTimeZone()
}

func (s_ *Session) SetTimeZone(zoneId string) {
	request := &rpc.TSSetTimeZoneReq{SessionId: s_.SessionId, TimeZone: zoneId}
	status, _ := s_.Client.SetTimeZone(default_Ctx, request)
	fmt.Printf("Settring Time ZoneId as {%v}, message: {%v}\n", zoneId, status.GetMessage())
	s_.ZoneId = zoneId
}

func (s_ *Session) checkSorted(timestamps []int64) bool {
	for i := 0; i < len(timestamps)-1; i++ {
		if timestamps[i] > timestamps[i+1] {
			return false
		}
	}
	return true
}

func (s_ *Session) verifySuccess(status int64) bool {
	return status == s_.SuccessCode
}