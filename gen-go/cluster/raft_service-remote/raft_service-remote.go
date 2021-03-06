// Autogenerated by Thrift Compiler (0.12.0)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package main

import (
        "context"
        "flag"
        "fmt"
        "math"
        "net"
        "net/url"
        "os"
        "strconv"
        "strings"
        "github.com/apache/thrift/lib/go/thrift"
	"rpc"
        "cluster"
)

var _ = rpc.GoUnusedProtection__

func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  HeartBeatResponse sendHeartbeat(HeartBeatRequest request)")
  fmt.Fprintln(os.Stderr, "  long startElection(ElectionRequest request)")
  fmt.Fprintln(os.Stderr, "  long appendEntries(AppendEntriesRequest request)")
  fmt.Fprintln(os.Stderr, "  long appendEntry(AppendEntryRequest request)")
  fmt.Fprintln(os.Stderr, "  void sendSnapshot(SendSnapshotRequest request)")
  fmt.Fprintln(os.Stderr, "  TSStatus executeNonQueryPlan(ExecutNonQueryReq request)")
  fmt.Fprintln(os.Stderr, "  long requestCommitIndex(Node header)")
  fmt.Fprintln(os.Stderr, "  string readFile(string filePath, long offset, int length)")
  fmt.Fprintln(os.Stderr, "  bool matchTerm(long index, long term, Node header)")
  fmt.Fprintln(os.Stderr, "  void removeHardLink(string hardLinkPath)")
  fmt.Fprintln(os.Stderr)
  os.Exit(0)
}

type httpHeaders map[string]string

func (h httpHeaders) String() string {
  var m map[string]string = h
  return fmt.Sprintf("%s", m)
}

func (h httpHeaders) Set(value string) error {
  parts := strings.Split(value, ": ")
  if len(parts) != 2 {
    return fmt.Errorf("header should be of format 'Key: Value'")
  }
  h[parts[0]] = parts[1]
  return nil
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  headers := make(httpHeaders)
  var parsedUrl *url.URL
  var trans thrift.TTransport
  _ = strconv.Atoi
  _ = math.Abs
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host and port")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.Var(headers, "H", "Headers to set on the http(s) request (e.g. -H \"Key: Value\")")
  flag.Parse()
  
  if len(urlString) > 0 {
    var err error
    parsedUrl, err = url.Parse(urlString)
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
    host = parsedUrl.Host
    useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http" || parsedUrl.Scheme == "https"
  } else if useHttp {
    _, err := url.Parse(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  if useHttp {
    trans, err = thrift.NewTHttpClient(parsedUrl.String())
    if len(headers) > 0 {
      httptrans := trans.(*thrift.THttpClient)
      for key, value := range headers {
        httptrans.SetHeader(key, value)
      }
    }
  } else {
    portStr := fmt.Sprint(port)
    if strings.Contains(host, ":") {
           host, portStr, err = net.SplitHostPort(host)
           if err != nil {
                   fmt.Fprintln(os.Stderr, "error with host:", err)
                   os.Exit(1)
           }
    }
    trans, err = thrift.NewTSocket(net.JoinHostPort(host, portStr))
    if err != nil {
      fmt.Fprintln(os.Stderr, "error resolving address:", err)
      os.Exit(1)
    }
    if framed {
      trans = thrift.NewTFramedTransport(trans)
    }
  }
  if err != nil {
    fmt.Fprintln(os.Stderr, "Error creating transport", err)
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.TProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewTCompactProtocolFactory()
    break
  case "simplejson":
    protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
    break
  case "json":
    protocolFactory = thrift.NewTJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
    Usage()
    os.Exit(1)
  }
  iprot := protocolFactory.GetProtocol(trans)
  oprot := protocolFactory.GetProtocol(trans)
  client := cluster.NewRaftServiceClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "sendHeartbeat":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "SendHeartbeat requires 1 args")
      flag.Usage()
    }
    arg41 := flag.Arg(1)
    mbTrans42 := thrift.NewTMemoryBufferLen(len(arg41))
    defer mbTrans42.Close()
    _, err43 := mbTrans42.WriteString(arg41)
    if err43 != nil {
      Usage()
      return
    }
    factory44 := thrift.NewTJSONProtocolFactory()
    jsProt45 := factory44.GetProtocol(mbTrans42)
    argvalue0 := cluster.NewHeartBeatRequest()
    err46 := argvalue0.Read(jsProt45)
    if err46 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.SendHeartbeat(context.Background(), value0))
    fmt.Print("\n")
    break
  case "startElection":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "StartElection requires 1 args")
      flag.Usage()
    }
    arg47 := flag.Arg(1)
    mbTrans48 := thrift.NewTMemoryBufferLen(len(arg47))
    defer mbTrans48.Close()
    _, err49 := mbTrans48.WriteString(arg47)
    if err49 != nil {
      Usage()
      return
    }
    factory50 := thrift.NewTJSONProtocolFactory()
    jsProt51 := factory50.GetProtocol(mbTrans48)
    argvalue0 := cluster.NewElectionRequest()
    err52 := argvalue0.Read(jsProt51)
    if err52 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.StartElection(context.Background(), value0))
    fmt.Print("\n")
    break
  case "appendEntries":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "AppendEntries requires 1 args")
      flag.Usage()
    }
    arg53 := flag.Arg(1)
    mbTrans54 := thrift.NewTMemoryBufferLen(len(arg53))
    defer mbTrans54.Close()
    _, err55 := mbTrans54.WriteString(arg53)
    if err55 != nil {
      Usage()
      return
    }
    factory56 := thrift.NewTJSONProtocolFactory()
    jsProt57 := factory56.GetProtocol(mbTrans54)
    argvalue0 := cluster.NewAppendEntriesRequest()
    err58 := argvalue0.Read(jsProt57)
    if err58 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.AppendEntries(context.Background(), value0))
    fmt.Print("\n")
    break
  case "appendEntry":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "AppendEntry requires 1 args")
      flag.Usage()
    }
    arg59 := flag.Arg(1)
    mbTrans60 := thrift.NewTMemoryBufferLen(len(arg59))
    defer mbTrans60.Close()
    _, err61 := mbTrans60.WriteString(arg59)
    if err61 != nil {
      Usage()
      return
    }
    factory62 := thrift.NewTJSONProtocolFactory()
    jsProt63 := factory62.GetProtocol(mbTrans60)
    argvalue0 := cluster.NewAppendEntryRequest()
    err64 := argvalue0.Read(jsProt63)
    if err64 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.AppendEntry(context.Background(), value0))
    fmt.Print("\n")
    break
  case "sendSnapshot":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "SendSnapshot requires 1 args")
      flag.Usage()
    }
    arg65 := flag.Arg(1)
    mbTrans66 := thrift.NewTMemoryBufferLen(len(arg65))
    defer mbTrans66.Close()
    _, err67 := mbTrans66.WriteString(arg65)
    if err67 != nil {
      Usage()
      return
    }
    factory68 := thrift.NewTJSONProtocolFactory()
    jsProt69 := factory68.GetProtocol(mbTrans66)
    argvalue0 := cluster.NewSendSnapshotRequest()
    err70 := argvalue0.Read(jsProt69)
    if err70 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.SendSnapshot(context.Background(), value0))
    fmt.Print("\n")
    break
  case "executeNonQueryPlan":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "ExecuteNonQueryPlan requires 1 args")
      flag.Usage()
    }
    arg71 := flag.Arg(1)
    mbTrans72 := thrift.NewTMemoryBufferLen(len(arg71))
    defer mbTrans72.Close()
    _, err73 := mbTrans72.WriteString(arg71)
    if err73 != nil {
      Usage()
      return
    }
    factory74 := thrift.NewTJSONProtocolFactory()
    jsProt75 := factory74.GetProtocol(mbTrans72)
    argvalue0 := cluster.NewExecutNonQueryReq()
    err76 := argvalue0.Read(jsProt75)
    if err76 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.ExecuteNonQueryPlan(context.Background(), value0))
    fmt.Print("\n")
    break
  case "requestCommitIndex":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "RequestCommitIndex requires 1 args")
      flag.Usage()
    }
    arg77 := flag.Arg(1)
    mbTrans78 := thrift.NewTMemoryBufferLen(len(arg77))
    defer mbTrans78.Close()
    _, err79 := mbTrans78.WriteString(arg77)
    if err79 != nil {
      Usage()
      return
    }
    factory80 := thrift.NewTJSONProtocolFactory()
    jsProt81 := factory80.GetProtocol(mbTrans78)
    argvalue0 := cluster.NewNode()
    err82 := argvalue0.Read(jsProt81)
    if err82 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    fmt.Print(client.RequestCommitIndex(context.Background(), value0))
    fmt.Print("\n")
    break
  case "readFile":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "ReadFile requires 3 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1, err84 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err84 != nil {
      Usage()
      return
    }
    value1 := cluster.Long(argvalue1)
    tmp2, err85 := (strconv.Atoi(flag.Arg(3)))
    if err85 != nil {
      Usage()
      return
    }
    argvalue2 := int32(tmp2)
    value2 := cluster.Int(argvalue2)
    fmt.Print(client.ReadFile(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "matchTerm":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "MatchTerm requires 3 args")
      flag.Usage()
    }
    argvalue0, err86 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err86 != nil {
      Usage()
      return
    }
    value0 := cluster.Long(argvalue0)
    argvalue1, err87 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err87 != nil {
      Usage()
      return
    }
    value1 := cluster.Long(argvalue1)
    arg88 := flag.Arg(3)
    mbTrans89 := thrift.NewTMemoryBufferLen(len(arg88))
    defer mbTrans89.Close()
    _, err90 := mbTrans89.WriteString(arg88)
    if err90 != nil {
      Usage()
      return
    }
    factory91 := thrift.NewTJSONProtocolFactory()
    jsProt92 := factory91.GetProtocol(mbTrans89)
    argvalue2 := cluster.NewNode()
    err93 := argvalue2.Read(jsProt92)
    if err93 != nil {
      Usage()
      return
    }
    value2 := argvalue2
    fmt.Print(client.MatchTerm(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "removeHardLink":
    if flag.NArg() - 1 != 1 {
      fmt.Fprintln(os.Stderr, "RemoveHardLink requires 1 args")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.RemoveHardLink(context.Background(), value0))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}
