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

		rh := fmt.Sprintf("bytes=%d-%d", start, end)
		req.Header.Set("Range", rh)
		lg("dl %s", rh)
		resp, err := c.Do(&req)
		if err != nil {
			// unexpected error
			err = fmt.Errorf("req of range=%d-%d err=%v", start, end, err)
			dl.setErr(err)
			return err
		}
		n, total, err := dl.readBody(resp, b, rh)
		lg("readbody n=%d total=%d err=%v", n, total, err)
		if err != nil {
			err = fmt.Errorf("read body err=%v", err)
			dl.setErr(err)
			return err
		}
		if n == 0 {
			log.Panicln("readbody returns 0 but no err")
		}

		dl.mu.Lock()
		// serialize disk i/o
		if _, err = dl.file.WriteAt(b[:n], start); err != nil {
			err = fmt.Errorf("write at=%d of n=%d err=%v", start, n, err)
		}

		if err == nil {
			end = start + int64(n) - 1 // actual end
			dl.setTotal(total)
			lg("%d-%d downloaded", start, end)
			dl.copied = append(dl.copied, offsetRange{start, end})
			dl.advanceCommit(rh)
		}
		dl.mu.Unlock()

		if err != nil {
			return err
		}
	}
}

func (dl *downloader) advanceCommit(rh string) {
	sort.Sort(offsetRangeSlice(dl.copied))
	lg("copied=%v", dl.copied)
	i := 0
	for ; i < len(dl.copied); i++ {
		r := dl.copied[i]
		if r.Start == dl.committed {
			dl.committed = r.End + 1
			lg("%s advance committed to %d", rh, dl.committed)
			continue
		}
		if r.Start < dl.committed {
			dl.setErr(fmt.Errorf("invalid range=(%d,%d) before committed=%d", r.Start, r.End, dl.committed))
		}
		break
	}
	dl.copied = dl.copied[i:]
}

func (dl *downloader) setErr(err error) {
	if dl.err == nil {
		lg("dl err=%v", err)
		dl.err = err
	}
}

func (dl *downloader) readBody(resp *http.Response, b []byte, rh string) (n int, total int64, err error) {
	lg("%s readbody resp=%+v", rh, resp)
	if (resp.StatusCode / 100) != 2 {
		return 0, 0, fmt.Errorf("stop because of resp status=%s", resp.Status)
	}

	cr := resp.Header.Get("Content-Range")
	if cr != "" {
		total = parseContentRange(cr)
	}

	n, err = io.ReadFull(resp.Body, b)
	resp.Body.Close()
	lg("%s read body n=%d err=%v", rh, n, err)
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, 0, fmt.Errorf("read body err=%v", err)
	}
	return n, total, nil
}

func parseContentRange(cr string) int64 {
	i := strings.Index(cr, "/")
	if i < 0 {
		lg("invalid content-range=%q, no slash", cr)
		return 0
	}
	s := cr[i+1:]
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		lg("invalid content-range=%q, parse total err=%v", cr, err)
		return 0
	}
	return n
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

func queryURL() (fn string, total int64, err error) {
	req := http.Request{
		Method: "HEAD",
		URL:    ur,
	}
	resp, err := c.Do(&req)
	if err != nil {
		return "", 0, fmt.Errorf("head url=%v err=%v", ur, err)
	}
	if resp.StatusCode != 200 {
		return "", 0, fmt.Errorf("bad response status=%s", resp.Status)
	}

	total = resp.ContentLength
	if total < 0 {
		// TODO: total probably should be -1
		total = 0
	}

	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		fn = parseContentDisposition(cd)
	}
	return fn, total, nil
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
