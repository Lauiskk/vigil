// Command capture records a live run of the pipeline to a trace file.
//
// The portfolio embeds a panel that streams from a deployed gateway. When
// there is no gateway to reach — because it is between deploys, or because
// nobody has stood one up yet — the panel replays this recording instead, and
// says so. A demo that quietly shows fabricated data would be worse than one
// that shows nothing; a demo that shows a real run and labels it as a
// recording is honest and still worth watching.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// frame is one server-sent event, with the offset from the start of the
// recording so a replayer can reproduce the original pacing.
type frame struct {
	OffsetMs int64           `json:"offsetMs"`
	Payload  json.RawMessage `json:"payload"`
}

type trace struct {
	CapturedAt time.Time `json:"capturedAt"`
	DurationMs int64     `json:"durationMs"`
	Source     string    `json:"source"`
	Frames     []frame   `json:"frames"`
}

func main() {
	var (
		url    = flag.String("url", "http://localhost:8080", "gateway base URL")
		out    = flag.String("out", "trace.json", "where to write the trace")
		window = flag.Duration("for", 90*time.Second, "how long to record")
		maxKB  = flag.Int("max-kb", 512, "stop early once the trace reaches this size")
	)
	flag.Parse()

	if err := run(*url, *out, *window, *maxKB); err != nil {
		fmt.Fprintln(os.Stderr, "capture:", err)
		os.Exit(1)
	}
}

func run(base, out string, window time.Duration, maxKB int) error {
	resp, err := http.Get(strings.TrimRight(base, "/") + "/api/stream")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gateway returned %s", resp.Status)
	}

	started := time.Now()
	deadline := started.Add(window)
	t := trace{CapturedAt: started.UTC(), Source: base}
	size := 0

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue // keepalive comments and blank separators
		}
		payload := strings.TrimPrefix(line, "data: ")
		if !json.Valid([]byte(payload)) {
			continue
		}

		t.Frames = append(t.Frames, frame{
			OffsetMs: time.Since(started).Milliseconds(),
			Payload:  json.RawMessage(payload),
		})
		size += len(payload)

		if time.Now().After(deadline) || size > maxKB*1024 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	t.DurationMs = time.Since(started).Milliseconds()
	body, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := os.WriteFile(out, body, 0o644); err != nil {
		return err
	}

	fmt.Printf("captured %d frames over %s into %s (%d KB)\n",
		len(t.Frames), time.Duration(t.DurationMs)*time.Millisecond, out, len(body)/1024)
	return nil
}
