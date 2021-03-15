package main

import (
	"client-go-iotdb/session"
	"client-go-iotdb/utils"
	"fmt"
)

func main() {
	s_ := session.NewDefaultSession()
	s_.Open(false)

	// set and delete storage groups
	s_.SetStorageGroup("root.sg_test_01")
	s_.SetStorageGroup("root.sg_test_02")
	s_.SetStorageGroup("root.sg_test_03")
	s_.SetStorageGroup("root.sg_test_04")
	s_.DeleteStorageGroup("root.sg_test_02")
	s_.DeleteStorageGroups([]string{"root.sg_test_03", "root.sg_test_04"})

	// setting time series.
	s_.CreateTimeSeries("root.sg_test_01.d_01.s_01", utils.TSDataType.BOOLEAN, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)
	s_.CreateTimeSeries("root.sg_test_01.d_01.s_02", utils.TSDataType.INT32, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)
	s_.CreateTimeSeries("root.sg_test_01.d_01.s_03", utils.TSDataType.INT64, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)

	// setting multiple time series once.
	ts_path_lst_ := []string{"root.sg_test_01.d_01.s_04", "root.sg_test_01.d_01.s_05", "root.sg_test_01.d_01.s_06",
		"root.sg_test_01.d_01.s_07", "root.sg_test_01.d_01.s_08", "root.sg_test_01.d_01.s_09"}
	data_type_lst_ := []int32{utils.TSDataType.FLOAT, utils.TSDataType.DOUBLE, utils.TSDataType.TEXT,
		utils.TSDataType.FLOAT, utils.TSDataType.DOUBLE, utils.TSDataType.TEXT}
	encoding_lst_ := make([]int32, 0)
	compressor_lst_ := make([]int32, 0)
	for i := 0; i < len(data_type_lst_); i++ {
		encoding_lst_ = append(encoding_lst_, utils.TSEncoding.PLAIN)
		compressor_lst_ = append(compressor_lst_, utils.Compressor.SNAPPY)
	}
	s_.CreateMultiTimeSeries(ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_)

	// delete time series
	s_.DeleteTimeSeries([]string{"root.sg_test_01.d_01.s_07", "root.sg_test_01.d_01.s_08", "root.sg_test_01.d_01.s_09"})

	// checking time series
	fmt.Println("s_07 expecting False, checking result: ", s_.CheckTimeSeriesExists("root.sg_test_01.d_01.s_07"))
	fmt.Println("s_03 expecting True, checking result: ", s_.CheckTimeSeriesExists("root.sg_test_01.d_01.s_03"))

	// insert one record into the database.
	measurements_ := []string{"s_01", "s_02", "s_03", "s_04", "s_05", "s_06"}
	values_ := []interface{}{false, 10, 11, 1.1, 10011.1, "test_record"}
	data_types_ := []int32{utils.TSDataType.BOOLEAN, utils.TSDataType.INT32, utils.TSDataType.INT64,
		utils.TSDataType.FLOAT, utils.TSDataType.DOUBLE, utils.TSDataType.TEXT}
	s_.InsertRecord("root.sg_test_01.d_01", measurements_, data_types_, values_, 1)

	// insert multiple records into database
	measurements_list_ := [][]string{{"s_01", "s_02", "s_03", "s_04", "s_05", "s_06"},
		{"s_01", "s_02", "s_03", "s_04", "s_05", "s_06"}}
	values_list_ := [][]interface{}{{false, 22, 33, 4.4, 55.1, "test_records01"},
		{true, 77, 88, 1.25, 8.125, "test_records02"}}
	data_type_list_ := [][]int32{data_types_, data_types_}
	device_ids_ := []string{"root.sg_test_01.d_01", "root.sg_test_01.d_01"}
	s_.InsertRecords(device_ids_, measurements_list_, data_type_list_, values_list_, []int64{2, 3})

	// close
	s_.Close(false)
}
