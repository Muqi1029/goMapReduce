package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/colinmarc/hdfs"
)

func main() {
	client, err := hdfs.New("localhost:9000")
	if err != nil {
		log.Fatalf("Failed to connect to HDFS: %v", err)
	}

	files, err := filepath.Glob("input/*.txt")
	if err != nil {
		log.Fatalf("Failed to find files: %v", err)
	}

	for _, localPath := range files {
		// 打开本地文件
		localFile, err := os.Open(localPath)
		if err != nil {
			log.Printf("Failed to open local file %s: %v", localPath, err)
			continue
		}

		// 使用匿名函数延迟关闭本地文件
		func() {
			defer localFile.Close()

			// 创建 HDFS 文件路径，假设 HDFS 目录为 /user/your-username/input/
			hdfsPath := strings.Replace(localPath, "input", "/user/muqi/input", 1)

			// 创建 HDFS 文件
			hdfsFile, err := client.Create(hdfsPath)
			if err != nil {
				log.Printf("Failed to create HDFS file %s: %v", hdfsPath, err)
				return
			}

			// 使用匿名函数延迟关闭 HDFS 文件
			defer hdfsFile.Close()

			// 复制本地文件内容到 HDFS 文件
			_, err = io.Copy(hdfsFile, localFile)
			if err != nil {
				log.Printf("Failed to copy data to HDFS file %s: %v", hdfsPath, err)
				return
			}

			fmt.Printf("Uploaded file: %s\n", hdfsPath)
		}()
	}
}
