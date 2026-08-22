package app

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
)

// warnIgnoredStdin предупреждает, когда данные пришли в stdin, но задан -file:
// файл побеждает, и переданный поток молча теряется.
func warnIgnoredStdin(fs *flag.FlagSet, path string) {
	explicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "file" {
			explicit = true
		}
	})
	if !explicit || path == "-" {
		return
	}
	st, err := os.Stdin.Stat()
	if err != nil || st.Mode()&os.ModeCharDevice != 0 {
		return // stdin — терминал, ничего не передавали
	}
	fmt.Fprintf(os.Stderr,
		"[send] ВНИМАНИЕ: в stdin есть данные, но задан -file %s — отправляется файл, stdin игнорируется\n"+
			"[send] чтобы отправить stdin, уберите -file или укажите -file -\n", path)
}

func openIn(path string) (io.Reader, func(), error) {
	if path == "-" || path == "" {
		return bufio.NewReader(os.Stdin), func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return bufio.NewReader(f), func() { f.Close() }, nil
}

func openOut(path string) (io.Writer, func(), error) {
	if path == "-" || path == "" {
		w := bufio.NewWriter(os.Stdout)
		return w, func() { w.Flush() }, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	w := bufio.NewWriter(f)
	return w, func() { w.Flush(); f.Close() }, nil
}
