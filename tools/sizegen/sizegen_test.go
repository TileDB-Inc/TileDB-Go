package main_test

import (
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
		pkgname  string
		export   bool
	}{
		{"defaults.txt", "", false},
		{"custom.txt", "custompkg", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.filename, func(t *testing.T) {
			t.Parallel()
			tmpdir, err := ioutil.TempDir("", "sizegen")
			if err != nil {
				t.Fatalf("could not create tempdir: %v", err)
			}
			outdir := filepath.Join(tmpdir, "testdata")
			if err := os.Mkdir(outdir, 0770); err != nil {
				t.Fatalf("could not create test subdir: %v", err)
			}
			out := filepath.Join(outdir, c.filename)
			args := []string{"run", binary, "--out", out, "--pkg", c.pkgname}
			if c.export {
				args = append(args, "--export=true")
			}
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
	cases := [][]string{
		{"--out=/at-the-root.go"}, // no params
		{"--out=package-name-is/invalid.go"},
		{"--out=/some/valid/path.go", "--pkg=invalid-package"},
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

func TestCantWrite(t *testing.T) {
	t.Parallel()
	tmpdir, err := ioutil.TempDir("", "sizegen")
	if err != nil {
		t.Fatalf("could not create tempdir: %v", err)
	}
	// The 'somepkg' directory is never created.
	fullPath := filepath.Join(tmpdir, "somepkg", "filename.go")
	cmd := exec.Command("go", "run", binary, "--out="+fullPath)
	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err == nil {
		t.Errorf("expected error writing to a missing directory")
	}
}

func read(t *testing.T, f string) string {
	in, err := ioutil.ReadFile(f)
	if err != nil {
		t.Fatalf("error opening %v: %v", f, err)
	}
	return string(in)
}
