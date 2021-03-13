package main

import (
	"client-go-iotdb/session"
)

func main() {
	session.NewSession().Open(true)
}
