package main

import (
	"encoding/json"
	"fmt"
	"goMapReduce/mr"
	"io/ioutil"

	"github.com/colinmarc/hdfs/v2"
)

const address = "localhost:9000"

func main() {
	client, err := hdfs.New(address)
	if err != nil {
		panic(err)
	}

	hdfsPath := "/user/muqi/tmp/mr-5-1"

	hdfsFile, err := client.Open(hdfsPath)
	if err != nil {
		panic(err)
	}
	defer hdfsFile.Close()
	contentBytes, err := ioutil.ReadAll(hdfsFile)
	if err != nil {
		panic(err)
	}
	var data []mr.KeyValue

	err = json.Unmarshal(contentBytes, &data)

	if err != nil {
		panic(err)
	}

	fmt.Println(data)

}
