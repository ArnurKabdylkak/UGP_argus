// Команда udpr — консольное приложение дата-диода: заворачивает данные в
// UDP + собственный Reliable Transport и доставляет их в точку назначения.
//
//	udpr send -host 10.0.0.2 -port 5555 -file data.bin
//	udpr recv -bind 0.0.0.0 -port 5555 -out data.bin
//	udpr selftest -loss 0.1
//
// Разбор аргументов и сборка зависимостей живут в internal/app.
package main

import (
	"os"

	"github.com/argus/udpr/internal/app"
)

func main() {
	os.Exit(app.Run(os.Args[1:]))
}
