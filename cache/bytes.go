package cache

import "bytes"

// bytesReader 返回读取 b 的 Reader，供日志解码使用。
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
