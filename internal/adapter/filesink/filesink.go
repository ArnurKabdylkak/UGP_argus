// Package filesink реализует port.SinkFactory: каждый принятый поток
// укладывается отдельным файлом в заданный каталог.
package filesink

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/argus/udpr/internal/port"
)

// Factory возвращает фабрику приёмников, пишущую потоки в каталог dir файлами
// вида <timestamp>-<session>.bin. Непустой notify получает имя созданного
// файла — через него команда сообщает оператору, куда легла сессия.
func Factory(dir string, notify func(port.SessionInfo, string)) port.SinkFactory {
	return func(info port.SessionInfo) (io.WriteCloser, error) {
		name := fmt.Sprintf("%s-%08x.bin", info.Started.UTC().Format("20060102T150405"), info.Session)
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		if notify != nil {
			notify(info, name)
		}
		return &bufferedFile{Writer: bufio.NewWriter(f), file: f}, nil
	}
}

// bufferedFile сбрасывает буфер на диск при закрытии сессии.
type bufferedFile struct {
	*bufio.Writer
	file *os.File
}

func (b *bufferedFile) Close() error {
	if err := b.Writer.Flush(); err != nil {
		b.file.Close()
		return err
	}
	return b.file.Close()
}
