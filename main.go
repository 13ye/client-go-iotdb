package main

import (
	"client-go-iotdb/session"
	"client-go-iotdb/utils"
)

func main() {
	session.NewSession().Open(true)
}
