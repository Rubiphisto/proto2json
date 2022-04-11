package main

import (
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

func registerPbFile(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if nil != err {
		return err
	}
	set := new(descriptorpb.FileDescriptorSet)
	if err := proto.Unmarshal(data, set); nil != err {
		return err
	}
	pb := set.GetFile()[0]
	fd, err := protodesc.NewFile(pb, protoregistry.GlobalFiles)
	if nil != err {
		return err
	}

	return protoregistry.GlobalFiles.RegisterFile(fd)
}

func convertToMap(descriptor protoreflect.MessageDescriptor, msg *dynamicpb.Message) map[string]interface{} {
	result := make(map[string]interface{})
	for i := 0; i < descriptor.Fields().Len(); i++ {
		field := descriptor.Fields().Get(i)
		if !msg.Has(field) {
			continue
		}
		var value interface{}
		if field.IsList() {
			list := []interface{}{}
			v := msg.Get(field).List()
			for j := 0; j < v.Len(); j++ {
				list = append(list, v.Get(j).Interface())
			}
			value = list
		} else {
			value = msg.Get(field).Interface()
		}
		result[string(field.Name())] = value
	}
	return result
}

func unmarshalProtoData(data []byte, msgName string) (map[string]interface{}, error) {
	var descriptor protoreflect.MessageDescriptor
	if desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(msgName)); nil != err {
		return nil, err
	} else {
		descriptor = desc.(protoreflect.MessageDescriptor)
	}
	msg := dynamicpb.NewMessage(descriptor)
	if err := proto.Unmarshal(data, msg); nil != err {
		return nil, err
	}
	return convertToMap(descriptor, msg), nil
}

func parseData(data []byte, msgName string) (map[string]interface{}, error) {
	var binary []byte
	var pos int
	var err error
	if '0' == data[0] && ('x' == data[1] || 'X' == data[1]) {
		binary = make([]byte, len(data)/2-1)
		pos, err = hex.Decode(binary, data[2:])
	} else {
		binary = make([]byte, len(data)/2)
		pos, err = hex.Decode(binary, data)
	}
	if nil != err {
		return nil, fmt.Errorf("error: pos:%v, err:%v, length: %v b:%v", pos, err, len(data), data[len(data)-1])
	}
	return unmarshalProtoData(binary, msgName)
}

type Data struct {
	line uint32
	data map[string]interface{}
}

type Marshaler interface {
	Marshal(map[string]interface{}) ([]byte, error)
}

type JsonMarshaler struct {
}

func (m *JsonMarshaler) Marshal(value map[string]interface{}) ([]byte, error) {
	return json.Marshal(value)
}

type Writer interface {
	Write([]byte) error
}

type FileWriter struct {
	mutex sync.Mutex
	file  *os.File
}

func (w *FileWriter) OpenFile(filename string) error {
	f, err := os.Create(filename)
	if nil != err {
		return err
	}
	w.file = f
	return nil
}

func (w *FileWriter) Write(data []byte) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if _, err := w.file.Write(data); nil != err {
		return err
	}
	if _, err := w.file.Write([]byte{'\n'}); nil != err {
		return nil
	}

	return nil
}

type ConsoleWriter struct {
}

func (w *ConsoleWriter) Write(data []byte) error {
	fmt.Printf("%s\n", data)
	return nil
}

func ReadData(dataList chan *Data, r io.Reader, fields []string) error {
	reader := csv.NewReader(r)

	var count uint32 = 0
	for {
		records, err := reader.Read()
		if io.EOF == err {
			break
		}
		if nil == records {
			return fmt.Errorf("faield to read csv data")
		}
		count++

		data := &Data{
			line: count,
			data: make(map[string]interface{}, len(fields)),
		}
		if len(records) < len(fields) {
			return fmt.Errorf("the %dth line hasn't enough field data", count)
		}
		for k, v := range fields {
			data.data[v] = records[k]
		}
		dataList <- data
	}
	return nil
}

func main() {
	var data string
	var srcFile string
	var dstFile string
	var receiverNum int
	var writerName string
	var marshalerName string
	var fieldsParam string
	var dataField string
	pbFile := flag.String("pb", "a", "specified the protobuf descriptor files")
	msgName := flag.String("name", "b", "specified the message name")
	//isHex := flag.Bool("hex", false, "the input data is hex format")
	flag.StringVar(&data, "data", "", "the input data")
	flag.StringVar(&srcFile, "srcfile", "", "specify the source data file")
	flag.StringVar(&dstFile, "dstfile", "", "specify the output file")
	flag.IntVar(&receiverNum, "recv", 10, "specify the parse thread count")

	flag.StringVar(&writerName, "writer", "console", "specify writer name, valid options: console, file")
	flag.StringVar(&marshalerName, "marshaler", "json", "specify marshaler name, valid options: json")
	flag.StringVar(&fieldsParam, "fields", "data", "specify the all fields name")
	flag.StringVar(&dataField, "dataField", "data", "specify the all fields name")
	flag.Parse()

	fields := strings.Split(fieldsParam, ",")

	hasDataField := false
	for _, v := range fields {
		if v == dataField {
			hasDataField = true
			break
		}
		if 0 == len(v) {
			panic(fmt.Errorf("the field's name can't be empty"))
		}
	}
	if !hasDataField {
		panic(fmt.Errorf("the data field '%s' isn't in fields list", dataField))
	}

	if err := registerPbFile(*pbFile); nil != err {
		panic(err)
	}

	if receiverNum <= 0 {
		panic(fmt.Errorf("the receiver count is less and equal than zero"))
	}
	dataList := make(chan *Data, receiverNum*2)
	wg := sync.WaitGroup{}

	var writer Writer = nil
	var marshaler Marshaler = nil

	if "console" == writerName {
		writer = &ConsoleWriter{}
	} else if "file" == writerName {
		fileWriter := &FileWriter{}
		if err := fileWriter.OpenFile(dstFile); nil != err {
			panic(err)
		}
		writer = fileWriter
	} else {
		panic(fmt.Errorf("invalid writer name:%v", writerName))
	}

	if "json" == marshalerName {
		marshaler = &JsonMarshaler{}
	} else {
		panic(fmt.Errorf("invalid marshaler name:%v", marshalerName))
	}

	if 0 != len(srcFile) {
		go func() {
			f, err := os.Open(srcFile)
			if nil != err {
				panic(err)
			}
			defer f.Close()

			if err := ReadData(dataList, f, fields); nil != err {
				panic(err)
			}
			close(dataList)
		}()
	} else {
		r := strings.NewReader(data)
		if err := ReadData(dataList, r, fields); nil != err {
			panic(err)
		}
		close(dataList)
	}

	for i := 0; i < receiverNum; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for data := range dataList {
				msg, err := parseData([]byte(data.data[dataField].(string)), *msgName)
				if nil != err {
					panic(err)
				}
				data.data[dataField] = msg
				text, err := marshaler.Marshal(data.data)
				if nil != err {
					panic(err)
				}
				if err := writer.Write(text); nil != err {
					panic(err)
				}
			}
		}(i)
	}

	wg.Wait()
}
