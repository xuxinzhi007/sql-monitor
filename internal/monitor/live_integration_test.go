package monitor

import (
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestLiveCaptureDoesNotChangeSQL(t *testing.T) {
	m, err := New(filepath.Join(t.TempDir(), "unused.log"), 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	lines := "{\"level\":\"DEBUG\",\"msg\":\"Lock already held by another instance\",\"caller\":\"lock.go:58\"}\n" +
		"{\"level\":\"WARN\",\"msg\":\"invalid thing mqtt reply\",\"caller\":\"thing.go:260\"}\n" +
		"{\"msg\":\"SQL Query\",\"sql\":\"SELECT 1\",\"elapsed\":\"1ms\"}\n" +
		"unstructured message\n"
	for _, b := range []byte(lines) {
		m.Consume([]byte{b})
	}
	sql := m.Snapshot()
	if len(sql.Events) != 1 || sql.Status.SQLObserved != 1 {
		t.Fatal("SQL capture changed")
	}
	body, err := json.Marshal(m.live.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var live struct {
		Events []struct {
			Message string `json:"message"`
		}
		Stats struct {
			Observed int `json:"observed"`
		}
		Anomalies []json.RawMessage
	}
	if err := json.Unmarshal(body, &live); err != nil {
		t.Fatal(err)
	}
	if len(live.Events) != 4 || live.Stats.Observed != 4 || len(live.Anomalies) != 1 {
		t.Fatalf("incorrect live capture: %s", body)
	}
}

func TestLiveOversizeAndPermanentEOF(t *testing.T) {
	m, err := New(filepath.Join(t.TempDir(), "unused.log"), 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.Consume([]byte(strings.Repeat("x", maxLine+100)))
	m.Consume([]byte("\nlast line without newline"))
	m.finishInput(io.EOF)
	data, _ := json.Marshal(m.live.snapshot())
	var got struct {
		Events []struct {
			Raw       string
			Truncated bool
		}
		Stats struct{ Observed int }
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Stats.Observed != 2 || len(got.Events) != 2 || !got.Events[0].Truncated || len(got.Events[0].Raw) > 16<<10 || got.Events[1].Raw != "last line without newline" {
		t.Fatal("oversize/EOF capture incorrect")
	}
	if m.Snapshot().Status.OversizeLines != 1 {
		t.Fatal("SQL oversize accounting changed")
	}
}
