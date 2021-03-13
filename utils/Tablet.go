package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

type Tablet struct {
	deviceId     string
	measurements []string
	dataTypes    []int32
	values       [][]interface{}
	timestamps   []int64
	columnNumber int
	rowNumber    int
}

func NewTablet(deviceId string, measurements []string, dataTypes []int32, values [][]interface{}, timestamps []int64) (t_ *Tablet) {
	if len(timestamps) != len(values) || len(dataTypes) != len(measurements) {
		fmt.Println("NewTablet: Length not match!")
		return nil
	}
	if !checkSorted(timestamps) {
		timestamps, values = sortBinding(timestamps, values)
	}
	// sorted
	return &Tablet{deviceId: deviceId, measurements: measurements, dataTypes: dataTypes, values: values, timestamps: timestamps, columnNumber: len(measurements), rowNumber: len(timestamps)}
}

func (t_ *Tablet) GetMeasurements() []string {
	return t_.measurements
}

func (t_ *Tablet) GetDataTypes() []int32 {
	return t_.dataTypes
}

func (t_ *Tablet) GetRowNumber() int {
	return t_.rowNumber
}

func (t_ *Tablet) GetColumnNumber() int {
	return t_.columnNumber
}

func (t_ *Tablet) GetDeviceId() string {
	return t_.deviceId
}

func (t_ *Tablet) GetTimestampsBinary() []byte {
	buf := new(bytes.Buffer)
	for _, v := range t_.timestamps {
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			fmt.Println("binary.Write failed:", err)
			return nil
		}
	}
	return buf.Bytes()
}

func (t_ *Tablet) GetValuesBinary() []byte {
	buf := new(bytes.Buffer)
	for j := 0; j < len(t_.measurements); j++ {
		if t_.dataTypes[j] == TSDataType.TEXT {
			for i := 0; i < len(t_.timestamps); i++ {
				if v_str, ok := t_.values[i][j].(string); ok {
					v_bytes := []byte(v_str)
					err := binary.Write(buf, binary.BigEndian, len(v_bytes))
					if err != nil {
						fmt.Println("binary.Write failed:", err)
						return nil
					}
					err = binary.Write(buf, binary.BigEndian, v_bytes)
					if err != nil {
						fmt.Println("binary.Write failed:", err)
						return nil
					}
				} else {
					fmt.Printf("value is not type string, i[%v] j[%v]\n", i, j)
					return nil
				}

			}
		} else {
			for i := 0; i < len(t_.timestamps); i++ {
				err := binary.Write(buf, binary.BigEndian, t_.values[i][j])
				if err != nil {
					fmt.Println("binary.Write failed:", err)
					return nil
				}
			}
		}
	}
	return buf.Bytes()
}

func sortBinding(timestamps []int64, values [][]interface{}) ([]int64, [][]interface{}) {
	type bind struct {
		time  int64
		value []interface{}
	}
	bindings := make([]bind, len(timestamps))
	for k, v := range values {
		bindings[k] = bind{timestamps[k], v}
	}
	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].time < bindings[j].time
	})
	for k, v := range bindings {
		timestamps[k] = v.time
		values[k] = v.value
	}
	return timestamps, values
}

func checkSorted(timestamps []int64) bool {
	for i := 0; i < len(timestamps)-1; i++ {
		if timestamps[i] > timestamps[i+1] {
			return false
		}
	}
	return true
}