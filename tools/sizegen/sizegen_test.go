package main_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	binary = "github.com/TileDB-Inc/TileDB-Go/tools/sizegen"
)

func TestGoldens(t *testing.T) {
	t.Parallel()
	cases := []struct {
		filename string
		params   []string
	}{
		{"defaults.txt", nil},
		{"custom.txt", []string{"--pkg", "custompkg", "--export", "--suffix=B0nkeyKong"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.filename, func(t *testing.T) {
			t.Parallel()
			outdir := filepath.Join(t.TempDir(), "testdata")
			if err := os.Mkdir(outdir, 0770); err != nil {
				t.Fatalf("could not create test subdir: %v", err)
			}
			out := filepath.Join(outdir, c.filename)
			args := []string{"run", binary, "--out", out}
			args = append(args, c.params...)
			cmd := exec.Command("go", args...)
			if err := cmd.Run(); err != nil {
				t.Fatalf("error executing case %v: %v", c, err)
			}

			got := read(t, out)
			want := read(t, filepath.Join("testdata", c.filename))
			if got != want {
				t.Errorf("file contents unexpected.\n\ngot:\n%v\n\nwant:\n%v", got, want)
			}
		})
	}
}

func TestBadPackages(t *testing.T) {
	t.Parallel()
	tmpdir := t.TempDir()
	cases := [][]string{
		{fmt.Sprintf("--out=%cat-the-root.go", filepath.Separator)},
		{"--out=" + filepath.Join(tmpdir, "package-name-is", "invalid.go")},
		{"--out=" + filepath.Join(tmpdir, "some", "valid", "path.go"), "--pkg=invalid-package"},
	}
	for _, c := range cases {
		t.Run(strings.Join(c, " "), func(t *testing.T) {
			t.Parallel()
			args := []string{"run", binary}
			args = append(args, c...)
			cmd := exec.Command("go", args...)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Errorf("expected error %v", c)
				return
			}
			outstr := string(output)
			if !strings.Contains(outstr, "invalid package name") {
				t.Errorf("expected 'invalid package name' error; got %v", outstr)
			}
		})
	}
}

func TestBadName(t *testing.T) {
	t.Parallel()
	tmpdir := filepath.Join(t.TempDir(), "output")
	if err := os.Mkdir(tmpdir, 0o770); err != nil {
		t.Fatalf("error creating test directory")
	}

	cmd := exec.Command("go", "run", ".", "--out", filepath.Join(tmpdir, "fakefile.go"), "--suffix=I have spaces")
	output, err := cmd.CombinedOutput()
	outstr := string(output)
	t.Log(outstr)
	if err == nil {
		t.Fatalf("expected error when writing a bad name")
	}
	if !strings.Contains(outstr, "invalid suffix") {
		t.Fatalf("expected 'invalid suffix' error; got %v", outstr)
	}
}

func TestCantWrite(t *testing.T) {
	t.Parallel()
	tmpdir := t.TempDir()
	// The 'somepkg' directory is never created.
	fullPath := filepath.Join(tmpdir, "somepkg", "filename.go")
	cmd := exec.Command("go", "run", binary, "--out="+fullPath)
	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err == nil {
		t.Errorf("expected error writing to a missing directory")
	}
}

func read(t testing.TB, f string) string {
	t.Helper()
	in, err := ioutil.ReadFile(f)
	if err != nil {
		t.Fatalf("error opening %v: %v", f, err)
	}
	return string(in)
}
