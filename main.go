package main

import (
	"client-go-iotdb/utils"
)

func main() {
	utils.NewSession().Open(true)
}
