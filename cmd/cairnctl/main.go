// Command cairnctl is a thin client for the coordinator's public API.
//
//	cairnctl [-url URL] mb <bucket>
//	cairnctl [-url URL] ls <bucket> [prefix]
//	cairnctl [-url URL] put <bucket> <key> <file>
//	cairnctl [-url URL] get <bucket> <key> [file]
//	cairnctl [-url URL] rm <bucket> <key>
//	cairnctl [-url URL] status
//	cairnctl [-url URL] locate <bucket> <key>
//
// -url defaults to $CAIRN_URL, then http://localhost:8080. File bodies are
// streamed in both directions; get writes to stdout when no file is given.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// ErrUsage means the command line could not be parsed.
var ErrUsage = errors.New("cairnctl: usage")

// ErrAPI means the coordinator answered with an error status.
var ErrAPI = errors.New("cairnctl: api error")

// arity is each subcommand's minimum and maximum positional count.
var arity = map[string][2]int{
	"mb": {1, 1}, "ls": {1, 2}, "put": {3, 3}, "get": {2, 3}, "rm": {2, 2}, "status": {0, 0}, "locate": {2, 2},
}

// command is one parsed invocation.
type command struct {
	URL  string
	Name string
	Args []string
}

func main() {
	cmd, err := parse(os.Args[1:], os.Getenv("CAIRN_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(context.Background(), cmd, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// parse reads the global flags and one subcommand. envURL is the -url
// default; the built-in default applies when it is empty.
func parse(args []string, envURL string) (command, error) {
	if envURL == "" {
		envURL = "http://localhost:8080"
	}
	fs := flag.NewFlagSet("cairnctl", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cmd command
	fs.StringVar(&cmd.URL, "url", envURL, "coordinator base URL (default $CAIRN_URL)")
	if err := fs.Parse(args); err != nil {
		return command{}, fmt.Errorf("%w: %v", ErrUsage, err)
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return command{}, fmt.Errorf("%w: missing subcommand", ErrUsage)
	}
	cmd.Name, cmd.Args = rest[0], rest[1:]
	want, ok := arity[cmd.Name]
	if !ok {
		return command{}, fmt.Errorf("%w: unknown subcommand %q", ErrUsage, cmd.Name)
	}
	if n := len(cmd.Args); n < want[0] || n > want[1] {
		return command{}, fmt.Errorf("%w: %s takes %d to %d arguments, got %d", ErrUsage, cmd.Name, want[0], want[1], n)
	}
	cmd.URL = strings.TrimRight(cmd.URL, "/")
	return cmd, nil
}

// run executes cmd against its coordinator and writes results to out.
func run(ctx context.Context, cmd command, out io.Writer) error {
	a := cmd.Args
	switch cmd.Name {
	case "mb":
		return call(ctx, http.MethodPut, cmd.URL+"/v1/"+a[0], nil, -1, io.Discard)
	case "ls":
		prefix := ""
		if len(a) == 2 {
			prefix = a[1]
		}
		return list(ctx, cmd.URL, a[0], prefix, out)
	case "put":
		f, err := os.Open(a[2])
		if err != nil {
			return err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			return err
		}
		return call(ctx, http.MethodPut, cmd.URL+objectPath(a[0], a[1]), f, st.Size(), out)
	case "get":
		if len(a) == 3 {
			f, err := os.Create(a[2])
			if err != nil {
				return err
			}
			if err := call(ctx, http.MethodGet, cmd.URL+objectPath(a[0], a[1]), nil, -1, f); err != nil {
				f.Close()
				return err
			}
			return f.Close()
		}
		return call(ctx, http.MethodGet, cmd.URL+objectPath(a[0], a[1]), nil, -1, out)
	case "rm":
		return call(ctx, http.MethodDelete, cmd.URL+objectPath(a[0], a[1]), nil, -1, io.Discard)
	case "status":
		return call(ctx, http.MethodGet, cmd.URL+"/cluster/status", nil, -1, out)
	case "locate":
		q := url.Values{"bucket": {a[0]}, "key": {a[1]}}
		return call(ctx, http.MethodGet, cmd.URL+"/cluster/locate?"+q.Encode(), nil, -1, out)
	}
	return fmt.Errorf("%w: unknown subcommand %q", ErrUsage, cmd.Name)
}

// objectPath is the API path for bucket/key with each key segment escaped.
func objectPath(bucket, key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return "/v1/" + bucket + "/" + strings.Join(segs, "/")
}

// call sends one request and streams a 2xx body to out. size is the
// Content-Length to declare for body, or -1 when there is none; a zero
// size sends an empty body rather than a chunked one the API would 411.
func call(ctx context.Context, method, u string, body io.Reader, size int64, out io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	if size == 0 {
		req.Body = http.NoBody
	} else if size > 0 {
		req.ContentLength = size
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return apiError(resp)
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		return fmt.Errorf("%s %s: read body: %w", method, u, err)
	}
	return nil
}

// apiError turns a non-2xx response into an ErrAPI carrying the body's code
// and message, or the raw body when it is not the API's error shape.
func apiError(resp *http.Response) error {
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return fmt.Errorf("%w: %d: read body: %w", ErrAPI, resp.StatusCode, err)
	}
	var e struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	if json.Unmarshal(b, &e) == nil && e.Error.Code != "" {
		return fmt.Errorf("%w: %d %s: %s", ErrAPI, resp.StatusCode, e.Error.Code, e.Error.Message)
	}
	return fmt.Errorf("%w: %d: %s", ErrAPI, resp.StatusCode, strings.TrimSpace(string(b)))
}

// list pages through every object under prefix, one "size\tkey" line each.
func list(ctx context.Context, base, bucket, prefix string, out io.Writer) error {
	var page struct {
		Objects []struct {
			Key  string `json:"key"`
			Size int64  `json:"size"`
		} `json:"objects"`
		Truncated      bool   `json:"truncated"`
		NextStartAfter string `json:"next_start_after"`
	}
	for after := ""; ; after = page.NextStartAfter {
		q := url.Values{}
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		if after != "" {
			q.Set("start_after", after)
		}
		var buf strings.Builder
		if err := call(ctx, http.MethodGet, base+"/v1/"+bucket+"/?"+q.Encode(), nil, -1, &buf); err != nil {
			return err
		}
		page.Objects, page.Truncated = nil, false
		if err := json.Unmarshal([]byte(buf.String()), &page); err != nil {
			return fmt.Errorf("listing %q: %w", buf.String(), err)
		}
		for _, o := range page.Objects {
			fmt.Fprintf(out, "%d\t%s\n", o.Size, o.Key)
		}
		if !page.Truncated {
			return nil
		}
	}
}
