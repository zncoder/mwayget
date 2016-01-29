package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO: support cookie

var (
	blockSize  = flag.Int("b", 1024*1024, "block size")
	numWorkers = flag.Int("n", 5, "number of concurrent workers")
	cont       = flag.Bool("c", false, "continue")
	filename   = flag.String("o", "", "output filename. the last part of the url is used if not set.")
	urlPath    = flag.String("u", "", "url")
	verbose    = flag.Bool("v", false, "verbose")

	ur *url.URL
	c  = http.Client{Timeout: 5 * time.Minute}
)

func lg(format string, arg ...interface{}) {
	if *verbose {
		log.Printf(format, arg...)
	}
}

type Hole struct {
	Start, End int64 // exclusive
}

type fileInfo struct {
	file     *os.File // nil means done
	filename string
	urlInfo
	mu    sync.Mutex
	holes []Hole
}

type urlInfo struct {
	remoteName   string
	total        int64
	supportRange bool
}

var fi fileInfo

func probeFileInfo() error {
	fi.total = -1

	if err := probeURL(); err != nil {
		return fmt.Errorf("probe url err=%v", err)
	}
	if fi.total < 0 {
		return fmt.Errorf("total size is unknown")
	}
	if fi.total == 0 {
		return fmt.Errorf("no content")
	}

	if err := setFilename(); err != nil {
		return fmt.Errorf("set filename err=%v", err)
	}

	if err := recoverHoles(); err != nil {
		return fmt.Errorf("recover holes err=%v", err)
	}
	return nil
}

type rangeResp struct {
	filename   string
	start, end int64
	total      int64
	body       io.ReadCloser
}

func probeURL() error {
	// implement HEAD with GET, which has wider support
	req := newRequest()

	// try range request first
	rr, err := doRequest(req, 0, 1)
	if err == nil {
		fi.remoteName = rr.filename
		fi.total = rr.total
		fi.supportRange = true
		rr.body.Close()
		return nil
	}

	// range is not supported
	req = newRequest()
	rr, err = doRequest(req, -1, 0)
	if err != nil {
		return fmt.Errorf("probe url with simple request err=%v", err)
	}
	fi.remoteName = rr.filename
	fi.total = rr.total
	fi.supportRange = false
	return nil
}

func doRequest(req *http.Request, start, end int64) (*rangeResp, error) {
	if start >= 0 {
		rh := fmt.Sprintf("bytes=%d-%d", start, end)
		req.Header.Set("Range", rh)
	}
	resp, err := c.Do(req)
	if err != nil {
		// unexpected error
		return nil, fmt.Errorf("http do err=%v", err)
	}
	logResp(resp)

	if (resp.StatusCode / 100) != 2 {
		return nil, fmt.Errorf("bad resp status=%s", resp.Status)
	}

	rr := &rangeResp{body: resp.Body}
	if a, b, tot, err := parseContentRange(resp.Header.Get("Content-Range")); err != nil {
		log.Printf("range request is not supported")
		rr.start, rr.end = -1, -1

		if s := resp.Header.Get("Content-Length"); s != "" {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				log.Fatalf("invalid content-length=%s", s)
			}
			rr.total = n
		} else {
			// total unknown
			rr.total = -1
		}
	} else if a != start {
		return nil, fmt.Errorf("start of range resp want=%d got=%d", start, a)
	} else {
		rr.start = a
		rr.end = b
		rr.total = tot
	}

	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		rr.filename = parseContentDisposition(cd)
	}
	return rr, nil
}

func newRequest() *http.Request {
	req := &http.Request{
		URL:    ur,
		Header: make(http.Header),
		// some server does not support multiple range requests over the same conn.
		Close: true,
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36")
	return req
}

func setFilename() error {
	switch {
	case *filename != "":
		fi.filename = *filename
	case fi.remoteName != "":
		fi.filename = fi.remoteName
	default:
		_, fn := filepath.Split(ur.Path)
		fi.filename = fn
	}
	if fi.filename == "" {
		return fmt.Errorf("cannot determine file name")
	}
	return nil
}

// the layout of a wip file,
//   <chunk><hole><chunk><hole>...<hole_info>
// hole_info is a 4096B block containing the json encoded []Hole, padded to 4K.
//
// We create a wip file, filename.wip, during download.  When download
// completes, we trim the trailing hole_info, and rename the wip file.
//
// NOTE: There is a window that the wip file might be corrupted. If the process
// is killed between the trim and the rename, the wip file won't have the
// trailing hole_info.
//
// If this is a problem, to eliminate this window, we can first rename the wip file
// to filename.wip.<final_size>, trim the hole_info from filename.wip.<final_size>,
// and rename filename.wip.<final_size> to filename. During recovery, if we have
// filename.wip, we redo the trim and rename; if we have filename.wip.<final_size>,
// we truncate the file to <final_size> and rename to filename.

const infoBlockSize = 4096

func recoverHoles() error {
	if st, err := os.Stat(fi.filename); err == nil {
		// done
		if st.Size() != fi.total {
			return fmt.Errorf("size of file=%s changed from=%d to=%d", fi.filename, st.Size(), fi.total)
		}
		return nil
	}

	wip := fi.filename + ".wip"
	f, err := openFile(wip)
	if err != nil {
		return err
	}
	fi.file = f

	sz := getFileSize()
	if sz < infoBlockSize {
		truncateTo(0)
		sz = 0
	}

	if sz == 0 {
		initHoles()
		writeInfoBlock()
	} else {
		readInfoBlock(sz - infoBlockSize)
	}
	return nil
}

func initHoles() {
	if !fi.supportRange {
		fi.holes = append(fi.holes, Hole{0, fi.total})
		return
	}

	w := int64(*numWorkers)
	if w > 99 {
		w = 99
	}
	if w <= 0 {
		w = 2
	}

	n := (fi.total + w - 1) / w
	if n < int64(*blockSize) {
		n = fi.total
	}
	var start, end int64
	for start < fi.total {
		end = start + n
		if end > fi.total {
			end = fi.total
		}
		fi.holes = append(fi.holes, Hole{start, end})
		start = end
	}
}

func writeInfoBlock() {
	b, err := json.Marshal(fi.holes)
	if err != nil {
		log.Fatalf("marshal holes err=%v", err)
	}
	n := len(b)
	if n > infoBlockSize {
		log.Fatalf("info block is too big, too many workers?\n%s", b)
	}

	var blk [infoBlockSize]byte
	copy(blk[:], b)
	for ; n < infoBlockSize; n++ {
		blk[n] = ' '
	}

	if _, err = fi.file.WriteAt(blk[:], fi.total); err != nil {
		log.Fatalf("write info block err=%v", err)
	}
}

func readInfoBlock(off int64) {
	var blk [infoBlockSize]byte
	if _, err := fi.file.ReadAt(blk[:], off); err != nil {
		log.Fatalf("read info block at=%d err=%v", off, err)
	}
	b := bytes.TrimSpace(blk[:])

	if err := json.Unmarshal(b, &fi.holes); err != nil {
		log.Fatalf("unmarshal info block=%s err=%v", b, err)
	}
}

func openFile(fn string) (*os.File, error) {
	openFlag := os.O_RDWR | os.O_CREATE
	if !*cont {
		openFlag |= os.O_TRUNC
	}
	f, err := os.OpenFile(fn, openFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("open file=%s err=%v", fn, err)
	}
	return f, nil
}

func getFileSize() int64 {
	st, err := fi.file.Stat()
	if err != nil {
		log.Fatalf("stat file=%s err=%v", fi.file.Name(), err)
	}
	return st.Size()
}

func truncateTo(sz int64) {
	if err := fi.file.Truncate(sz); err != nil {
		log.Fatalf("truncate file=%s to size=%d err=%v", fi.file.Name(), sz, err)
	}
}

func finishDownload() error {
	wip := fi.file.Name()
	if err := fi.file.Close(); err != nil {
		return fmt.Errorf("close wip file=%s err=%v", wip, err)
	}

	if err := os.Truncate(wip, fi.total); err != nil {
		return fmt.Errorf("truncate wip file=%s to size=%d err=%v", wip, fi.total, err)
	}
	if err := os.Rename(wip, fi.filename); err != nil {
		return fmt.Errorf("rename wip=%s to file=%s err=%v", wip, fi.filename, err)
	}
	return nil
}

type downloader struct {
	hi         int
	start, end int64
	name       string

	off int64
	err error
}

func newDownloader(hi int) *downloader {
	return &downloader{
		hi:    hi,
		start: fi.holes[hi].Start,
		end:   fi.holes[hi].End,
		name:  fmt.Sprintf("dl:%d-%d", fi.holes[hi].Start, fi.holes[hi].End),
		off:   fi.holes[hi].Start,
	}
}

func (dl *downloader) Run() {
	req := newRequest()
	if fi.supportRange {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", dl.start, dl.end))
	}

	resp, err := c.Do(req)
	if err != nil {
		log.Printf("%s: do req err=%v", dl.name, err)
		return
	}
	defer resp.Body.Close()
	logResp(resp)
	if (resp.StatusCode / 100) != 2 {
		log.Printf("%s: bad resp status=%s", dl.name, resp.Status)
		return
	}

	bufSize := func(sz int64) int {
		n := *blockSize
		if int64(n) > sz {
			n = int(sz)
		}
		return n
	}

	n := bufSize(dl.end - dl.start)
	buf := make([]byte, n)

	for dl.err == nil && dl.off < dl.end {
		b := buf
		n = bufSize(dl.end - dl.off)
		b = buf[:n]
		if n, dl.err = io.ReadFull(resp.Body, b); n > 0 {
			dl.advance(b[:n])
		}
	}
	if dl.err != nil {
		log.Printf("%s: stop at %d, err=%v", dl.name, dl.off, dl.err)
	} else {
		log.Printf("%s: done", dl.name)
	}
}

func (dl *downloader) advance(b []byte) {
	// serialize disk io
	fi.mu.Lock()
	defer fi.mu.Unlock()

	_, err := fi.file.WriteAt(b, dl.off)
	if err != nil {
		log.Printf("%s: write at offset=%d err=%v", dl.name, dl.off, err)
		if dl.err == nil {
			dl.err = err
		}
		return
	}

	dl.off += int64(len(b))
	fi.holes[dl.hi].Start = dl.off
	lg("%s: advance to %d", dl.name, dl.off)
	writeInfoBlock()
}

func main() {
	flag.Parse()

	var err error
	if ur, err = url.Parse(*urlPath); err != nil {
		log.Fatalf("invalid url=%s err=%v", *urlPath, err)
	}

	if err = probeFileInfo(); err != nil {
		log.Fatalf("probe file info err=%v", err)
	}
	if fi.file == nil {
		log.Printf("finish getting file=%s size=%d", fi.filename, fi.total)
		return
	}
	lg("fileinfo=%+v", &fi)

	w := len(fi.holes)
	log.Printf("to get file=%s with %d workers in ranges=%v", fi.filename, w, fi.holes)

	var wg sync.WaitGroup
	wg.Add(w)
	for i := 0; i < w; i++ {
		dl := newDownloader(i)
		go func() {
			defer wg.Done()
			dl.Run()
		}()
	}
	wg.Wait()

	if err = finishDownload(); err != nil {
		log.Fatalf("finish download err=%v", err)
	}
	log.Printf("finish getting file=%s", fi.filename)
}

func parseContentRange(cr string) (start, end, total int64, err error) {
	if cr == "" {
		return 0, 0, 0, fmt.Errorf("no content-range")
	}

	s := cr

	i := strings.LastIndex(s, " ")
	if i >= 0 {
		s = s[i+1:]
	}

	fs := make([]string, 3)
	// a-b/c
	i = strings.Index(s, "-")
	if i < 0 {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range=%s", cr)
	}
	fs[0] = s[:i]
	s = s[i+1:]

	i = strings.Index(s, "/")
	if i < 0 {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range=%s", cr)
	}
	fs[1], fs[2] = s[:i], s[i+1:]

	is := make([]int64, 3)
	for i, s := range fs {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("malformed range Content-Range=%s err=%v", cr, err)
		}
		is[i] = n
	}
	return is[0], is[1], is[2], nil
}

func parseContentDisposition(cd string) string {
	_, params, err := mime.ParseMediaType(cd)
	if err != nil {
		log.Printf("parse content-disposition=%s err=%v", cd, err)
		return ""
	}
	if fn, ok := params["filename"]; ok {
		fn2, err := url.QueryUnescape(fn)
		if err != nil {
			log.Printf("unescape filename=%s err=%v", fn, err)
			return fn
		}
		return fn2
	}
	return ""
}

func logResp(resp *http.Response) {
	if !*verbose {
		return
	}
	var buf bytes.Buffer
	fmt.Fprintln(&buf, resp.Status)
	for k, vs := range resp.Header {
		for _, v := range vs {
			fmt.Fprintf(&buf, "  %s: %s\n", k, v)
		}
	}
	log.Printf("response:\n%s", buf.Bytes())
}
