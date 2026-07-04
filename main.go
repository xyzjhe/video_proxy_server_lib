package main

import "C"
import (
	_ "bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strconv"
	_ "strings"
	"sync"
	"time"
)

// 全局配置
const (
	PartSize = 1024 * 1024 // 1MB
)

// 使用 sync.Map 来安全地在并发环境下存储和检索 URL 及 Headers
var urlMap = sync.Map{}
var headerMap = sync.Map{}

var Port = 12345

// 可以根据 CPU 核心数调整
var ThreadNum = 16

// findAvailablePort 从指定端口开始，找到一个可用端口
func findAvailablePort(port int) int {
	for {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err == nil {
			ln.Close() // 探测成功，关闭监听，端口可用
			return port
		}
		log.Printf("端口 %d 已被占用，尝试下一个端口...", port)
		port++
	}
}

var (
	rangeRegexp        = regexp.MustCompile(`bytes=(\d+)-(\d*)`)
	contentRangeRegexp = regexp.MustCompile(`bytes \d+-\d+/(\d+)`)

	// 初始化全局 http.Client
	// 配置高并发流媒体场景下的专属连接池参数
	globalClient = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second, // 连接超时
				KeepAlive: 30 * time.Second, // 保活时间
			}).DialContext,
			MaxIdleConns:          100,              // 全局最大空闲连接数
			MaxIdleConnsPerHost:   32,               // 每个域名最大空闲连接数（多线程点播核心调优）
			IdleConnTimeout:       90 * time.Second, // 空闲连接超时释放
			TLSHandshakeTimeout:   5 * time.Second,  // TLS 握手超时
			ExpectContinueTimeout: 1 * time.Second,
			DisableKeepAlives:     false, // 必须开启 Keep-Alive 才能复用连接
		},
		Timeout: 30 * time.Second, // 单次分片下载总超时
	}
)

func main() {
	Java_com_github_catvod_spider_LuProxyNative_StartServer()
}

//export Java_com_github_catvod_spider_LuProxyNative_StartServer
func Java_com_github_catvod_spider_LuProxyNative_StartServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "ser200")
	})

	http.HandleFunc("/buildUrl", buildUrl)
	http.HandleFunc("/proxy", proxyHandler)

	Port = findAvailablePort(Port)

	log.Printf("启动服务 on %d", Port)
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", Port),
		MaxHeaderBytes:    10 * 1024 * 1024, // 10MB
		ReadHeaderTimeout: 30 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatal("启动服务出错:", err)
	}
}

type Request struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Key     string            `json:"key"`
}

func buildUrl(w http.ResponseWriter, r *http.Request) {

	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	urlMap.Clear()
	headerMap.Clear()

	urlMap.Store(req.Key, req.URL)
	headerMap.Store(req.Key, req.Headers)

}

func proxyHandler(w http.ResponseWriter, r *http.Request) {

	// 1. 获取 Base64 编码的参数
	key := r.URL.Query().Get("key")
	threadsParam := r.URL.Query().Get("threads")

	// 设置线程数
	if threadsParam != "" {
		ThreadNum, _ = strconv.Atoi(threadsParam)
	}
	log.Printf("启动线程数%d", ThreadNum)

	url, _ := urlMap.Load(key)
	log.Printf("URL: %s", url)

	headers, _ := headerMap.Load(key)

	log.Printf("headers: %s", headers)
	proxyAsync(url.(string), headers.(map[string]string), r, w)
}

func proxyAsync(url string, headers map[string]string, req *http.Request, w http.ResponseWriter) {
	// 获取客户端的 Context
	ctx := req.Context()

	rangeHeader := req.Header.Get("Range")
	if rangeHeader == "" {
		rangeHeader = "bytes=0-"
	}

	// 复制 headers 并添加 Range
	newHeaders := make(map[string]string)
	for k, v := range headers {
		newHeaders[k] = v
	}
	newHeaders["Range"] = rangeHeader

	startPoint, endPoint := parseRangePoint(rangeHeader)

	// 获取文件信息
	info, err := getInfo(url, newHeaders)
	if err != nil {
		http.Error(w, "Failed to get info: "+err.Error(), http.StatusInternalServerError)
		return
	}

	contentLength := getContentLength(info)
	finalEndPoint := endPoint
	if endPoint == -1 {
		finalEndPoint = contentLength - 1
	}

	// 设置响应头
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Length", strconv.FormatInt(finalEndPoint-startPoint+1, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startPoint, finalEndPoint, contentLength))
	if contentType := info["Content-Type"]; contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(http.StatusPartialContent)

	var currentStart = startPoint
	//var wg sync.WaitGroup

	for currentStart <= finalEndPoint {

		// 检查客户端是否已经关闭了播放器
		select {
		case <-ctx.Done():
			log.Printf("客户端已断开连接，停止代理分片。")
			return
		default:
		}

		// 每轮重新创建 channel，避免复用问题
		channels := make([]chan []byte, ThreadNum)
		for i := range channels {
			channels[i] = make(chan []byte, 1) // 带缓冲，防止 goroutine 泄漏
		}

		var wg sync.WaitGroup
		launched := 0 // 记录本轮实际启动的 goroutine 数量

		for i := 0; i < ThreadNum; i++ {
			if currentStart > finalEndPoint {
				break
			}
			chunkStart := currentStart
			chunkEnd := miner(currentStart+PartSize-1, finalEndPoint)

			wg.Add(1)
			go func(idx int, start, end int64) {
				defer wg.Done()
				defer close(channels[idx]) // ✅ goroutine 结束后关闭 channel
				getVideoStream(ctx, start, end, contentLength, url, newHeaders, channels[idx])
			}(i, chunkStart, chunkEnd)

			currentStart = chunkEnd + 1
			launched++
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			log.Printf("w 不支持 Flusher 接口")
		}
		// 只接收实际启动的 goroutine 数量的数据
		for i := 0; i < launched; i++ {
			data, ok := <-channels[i]
			if !ok {
				continue
			}
			if len(data) > 0 {
				w.Write(data)
				flusher.Flush()
			}
		}

		wg.Wait() // 确保本轮所有 goroutine 彻底完成后再进入下一轮
	}

}

func detectMaliciousPrefix(data []byte) int {
	buffer := make([]byte, 64)
	copy(buffer, data)

	if isValidVideoHeader(buffer) {
		return 0
	}

	searchLimit := miner(256, int64(len(data)))
	searchBuffer := make([]byte, searchLimit)
	copy(searchBuffer, data)

	for offset := 1; offset < int(searchLimit)-16; offset++ {
		if isValidVideoHeader(searchBuffer[offset:]) {
			log.Printf("发现合法视频头位于偏移 %d，疑似被插入恶意前缀！", offset)
			return offset
		}
	}
	return 0
}

func isValidVideoHeader(data []byte) bool {
	if len(data) < 8 {
		return false
	}

	// MP4 / MOV: ftyp
	if len(data) >= 8 && data[4] == 'f' && data[5] == 't' && data[6] == 'y' && data[7] == 'p' {
		size := int64(data[0])<<24 | int64(data[1])<<16 | int64(data[2])<<8 | int64(data[3])
		if size >= 8 && size <= 0x100000 {
			return true
		}
	}

	// AVI: RIFF
	if len(data) >= 4 && data[0] == 'R' && data[1] == 'I' && data[2] == 'F' && data[3] == 'F' {
		return true
	}

	// MKV
	if len(data) >= 4 && data[0] == 0x1A && data[1] == 0x45 && data[2] == 0xDF && data[3] == 0xA3 {
		return true
	}

	// FLV
	if len(data) >= 4 && data[0] == 'F' && data[1] == 'L' && data[2] == 'V' && data[3] == 0x01 {
		return true
	}

	return false
}

func parseRangePoint(rangeHeader string) (int64, int64) {
	re := rangeRegexp
	matches := re.FindStringSubmatch(rangeHeader)
	if len(matches) < 2 {
		return 0, -1
	}
	start, _ := strconv.ParseInt(matches[1], 10, 64)
	end := int64(-1)
	if matches[2] != "" {
		end, _ = strconv.ParseInt(matches[2], 10, 64)
	}
	return start, end
}

func getInfo(url string, headers map[string]string) (map[string]string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Range", "bytes=0-0") // 1MB

	resp, err := globalClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	info := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			info[k] = v[0]
		}
	}
	return info, nil
}

func getContentLength(info map[string]string) int64 {
	// 如果 Content-Length 不存在，尝试从 Content-Range 解析
	if contentRange, ok := info["Content-Range"]; ok {
		return parseContentLengthFromRange(contentRange)
	}

	return 0
}
func parseContentLengthFromRange(contentRange string) int64 {
	// Content-Range 格式: bytes start-end/total
	re := contentRangeRegexp
	matches := re.FindStringSubmatch(contentRange)
	if len(matches) >= 2 {
		total, err := strconv.ParseInt(matches[1], 10, 64)
		if err == nil {
			return total
		}
	}
	return 0
}

func getVideoStream(ctx context.Context, start, end int64, contentLength int64, url string, headers map[string]string, ch chan []byte) {
	if start > contentLength {
		return
	}
	log.Printf("开始获取视频片段 %d-%d", start, end)
	// 使用 NewRequestWithContext，将客户端状态与源站请求绑定
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("构造请求出错: %v", err)
		return
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := globalClient.Do(req)
	if err != nil {
		log.Printf("请求视频出错: %v", err)
		return // 修复：必须 return，防止后续 defer 空指针 Panic
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("读取响应体出错: %v", err)
		return
	}

	if start == 0 {
		offset := detectMaliciousPrefix(data)
		ch <- data[offset:]
	} else {
		ch <- data
	}
}

func miner(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
