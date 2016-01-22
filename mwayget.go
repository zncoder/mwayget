package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	blockSize = flag.Int64("b", 1024*1024, "block size")
	numBlocks = flag.Int("n", 10, "number of concurrent blocks")
	cont      = flag.Bool("c", false, "continue")
	filename  = flag.String("o", "", "output filename. the last part of the url is used if not set.")
	urlPath   = flag.String("u", "", "url")
	verbose   = flag.Bool("v", false, "verbose")

	ur *url.URL
	c  = http.Client{Timeout: 5 * time.Minute}
)

func lg(format string, arg ...interface{}) {
	if *verbose {
		log.Printf(format, arg...)
	}
}

type offsetRange struct {
	Start, End int64 // inclusive
}

type offsetRangeSlice []offsetRange

func (ors offsetRangeSlice) Less(i, j int) bool { return ors[i].Start < ors[j].Start }
func (ors offsetRangeSlice) Len() int           { return len(ors) }
func (ors offsetRangeSlice) Swap(i, j int)      { ors[i], ors[j] = ors[j], ors[i] }

type downloader struct {
	file      *os.File
	mu        sync.Mutex
	total     int64
	committed int64
	offset    int64
	copied    []offsetRange
	err       error
}

// TODO:
//  - support cookies

func main() {
	flag.Parse()

	var err error
	if ur, err = url.Parse(*urlPath); err != nil {
		log.Fatalf("invalid url=%s err=%v", *urlPath, err)
	}

	dl := NewDownloader()
	lg("dl starts with filename=%s committed=%d total=%d", *filename, dl.committed, dl.total)

	var wg sync.WaitGroup
	wg.Add(*numBlocks)
	for i := 0; i < *numBlocks; i++ {
		go func(i int) {
			defer wg.Done()
			err := dl.Run()
			lg("dl-%d stopped err=%v", i, err)
		}(i)
	}
	wg.Wait()

	if err = dl.Close(); err != nil {
		log.Fatalf("close err=%v", err)
	}
	if dl.committed != dl.total {
		log.Fatalf("want=%d got=%d", dl.total, dl.committed)
	}
}

func getFilename() string {
	if *filename != "" {
		return *filename
	}
	_, fn := filepath.Split(ur.Path)
	if fn == "" {
		log.Fatalf("cannot derive file name from url path=%s", ur.Path)
	}
	return fn
}

func openFile() *os.File {
	openFlag := os.O_WRONLY | os.O_CREATE
	if !*cont {
		openFlag |= os.O_TRUNC
	}
	f, err := os.OpenFile(*filename, openFlag, 0600)
	if err != nil {
		log.Fatalf("open file=%s err=%v", *filename, err)
	}
	return f
}

func getFileSize(file *os.File) int64 {
	fi, err := file.Stat()
	if err != nil {
		log.Fatalf("stat file=%s err=%v", file.Name(), err)
	}
	return fi.Size()
}

func (dl *downloader) Run() error {
	req := http.Request{
		URL:    ur,
		Header: make(http.Header),
	}

	b := make([]byte, int(*blockSize))
	for {
		dl.mu.Lock()
		start, end := dl.offset, dl.offset+*blockSize-1
		if dl.total > 0 && end >= dl.total {
			end = dl.total - 1
		}
		dl.offset = end + 1
		err := dl.err
		dl.mu.Unlock()

		if err != nil {
			return fmt.Errorf("dl done at start=%d end=%d with err=%v", start, end, err)
		}
		if start > end {
			return fmt.Errorf("dl reaches end start=%d end=%d", start, end)
		}

		rr, err := doRangeRequest(&req, start, end)
		if err != nil {
			err = fmt.Errorf("req of range=%d-%d err=%v", start, end, err)
			dl.setErr(err)
			return err
		}
		lg("range resp=%+v", rr)

		n, err := dl.readBody(rr.body, b)
		if err != nil {
			err = fmt.Errorf("read body err=%v", err)
			dl.setErr(err)
			return err
		}
		if n == 0 {
			log.Panicln("readbody returns 0 but no err")
		}
		if n != int(rr.end-rr.start+1) {
			log.Panicf("readbody got=%d/%d-%d want=%d", rr.end-rr.start+1, rr.start, rr.end, n)
		}

		dl.mu.Lock()
		// serialize disk i/o
		if _, err = dl.file.WriteAt(b[:n], start); err != nil {
			err = fmt.Errorf("write at=%d of n=%d err=%v", start, n, err)
		}

		if err == nil {
			end = rr.end
			dl.setTotal(rr.total)
			lg("%d-%d downloaded", start, end)
			dl.copied = append(dl.copied, offsetRange{start, end})
			dl.advanceCommit()
		}
		dl.mu.Unlock()

		if err != nil {
			return err
		}
	}
}

func (dl *downloader) advanceCommit() {
	begin := dl.committed
	sort.Sort(offsetRangeSlice(dl.copied))
	lg("copied=%v", dl.copied)
	i := 0
	for ; i < len(dl.copied); i++ {
		r := dl.copied[i]
		if r.Start == dl.committed {
			dl.committed = r.End + 1
			continue
		}
		if r.Start < dl.committed {
			dl.setErr(fmt.Errorf("invalid range=(%d,%d) before committed=%d", r.Start, r.End, dl.committed))
		}
		break
	}
	dl.copied = dl.copied[i:]
	if dl.committed > begin {
		log.Printf("advance committed to %d", dl.committed)
	}
}

func (dl *downloader) setErr(err error) {
	if dl.err == nil {
		lg("dl err=%v", err)
		dl.err = err
	}
}

func (dl *downloader) readBody(body io.ReadCloser, b []byte) (int, error) {
	n, err := io.ReadFull(body, b)
	body.Close()
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, fmt.Errorf("read body err=%v", err)
	}
	return n, nil
}

func parseContentRange(cr string) (start, end, total int64, err error) {
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

func firstErr(err ...error) error {
	for _, e := range err {
		if e != nil {
			return e
		}
	}
	return nil
}

func (dl *downloader) Close() error {
	err1 := dl.file.Truncate(dl.committed)
	err2 := dl.file.Close()
	return firstErr(err1, err2)
}

func (dl *downloader) setTotal(total int64) {
	switch {
	case total <= 0:
		return

	case dl.total == 0:
		dl.total = total

	case dl.total != total:
		log.Fatalf("total changed from %d to %d", dl.total, total)
	}
}

func NewDownloader() *downloader {
	sfn, total, err := queryURL()
	if err != nil {
		log.Fatalf("head url=%v err=%v", ur, err)
	}
	log.Printf("filename=%s total=%d", sfn, total)

	var committed int64
	*filename = getFilename()
	fi, err := os.Stat(*filename)
	if err != nil {
		// file does not exist
		if sfn != "" {
			*filename = sfn
		}
		*cont = false
	} else if *cont {
		committed = fi.Size()
	}

	return &downloader{
		file:      openFile(),
		committed: committed,
		offset:    committed,
		total:     total,
	}
}

type rangeResp struct {
	filename   string
	start, end int64
	total      int64
	body       io.ReadCloser
}

func doRangeRequest(req *http.Request, start, end int64) (*rangeResp, error) {
	rh := fmt.Sprintf("bytes=%d-%d", start, end)
	req.Header.Set("Range", rh)
	resp, err := c.Do(req)
	if err != nil {
		// unexpected error
		return nil, fmt.Errorf("http do %s err=%v", rh, err)
	}

	if (resp.StatusCode / 100) != 2 {
		return nil, fmt.Errorf("bad resp status=%s", resp.Status)
	}

	rr := &rangeResp{body: resp.Body}
	if rr.start, rr.end, rr.total, err = parseContentRange(resp.Header.Get("Content-Range")); err != nil {
		return nil, err
	}
	if rr.start != start {
		return nil, fmt.Errorf("start of range resp want=%d got=%d", start, rr.start)
	}
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		rr.filename = parseContentDisposition(cd)
	}
	return rr, nil
}

func queryURL() (fn string, total int64, err error) {
	req := http.Request{
		// implement HEAD with GET, which has wider support
		Method: "GET",
		URL:    ur,
		Header: make(http.Header),
	}

	rr, err := doRangeRequest(&req, 0, 1)
	if err != nil {
		return "", 0, err
	}
	return rr.filename, rr.total, nil
}

func parseContentDisposition(cd string) string {
	tok := `filename="`
	i := strings.Index(cd, tok)
	if i < 0 {
		return ""
	}
	i += len(tok)
	if !strings.HasSuffix(cd, `"`) {
		return ""
	}
	return cd[i : len(cd)-1]
}
