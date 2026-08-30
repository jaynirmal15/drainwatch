package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// WriteJSON writes v to path as indented JSON with a trailing newline, creating
// parent directories as needed. It writes via a temporary file and renames, so
// a partially written report can never be mistaken for a complete one.
func WriteJSON(path string, v any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cannot create report directory %s: %w (invariant: the output directory must be creatable; check filesystem permissions and --out)", dir, err)
	}
	buf, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot encode report as JSON: %w (invariant: every report value must be JSON-encodable)", err)
	}
	buf = append(buf, '\n')

	tmp, err := os.CreateTemp(dir, ".drainwatch-*.tmp")
	if err != nil {
		return fmt.Errorf("cannot create temporary file in %s: %w (invariant: the output directory must be writable; check filesystem permissions and --out)", dir, err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("cannot write %s: %w (invariant: the report must be written in full or not at all)", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("cannot close %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("cannot move report into place at %s: %w", path, err)
	}
	return nil
}

// ReadReport loads a report.json written by WriteJSON.
func ReadReport(path string) (*Report, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read report %s: %w", path, err)
	}
	var r Report
	dec := json.NewDecoder(bytes.NewReader(buf))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("report %s does not match the drainwatch schema: %w (invariant: report.json must round-trip through the schema of the binary reading it)", path, err)
	}
	return &r, nil
}
