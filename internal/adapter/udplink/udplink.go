// Package udplink реализует port.Link поверх UDP-сокета.
package udplink

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/argus/udpr/internal/port"
)

// Link — канал передачи датаграмм поверх UDP.
type Link struct {
	conn      *net.UDPConn
	connected bool // сокет привязан к одному узлу (Dial)
	buf       []byte
}

// Dial открывает канал в сторону remote. Непустой local закрепляет локальный
// адрес и порт: без этого ОС выдаёт эфемерный порт, который меняется при
// каждом запуске, и обратный поток нельзя описать постоянным правилом
// на межсетевом экране.
func Dial(remote, local string) (*Link, error) {
	raddr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		return nil, err
	}
	var laddr *net.UDPAddr
	if local != "" {
		laddr, err = net.ResolveUDPAddr("udp", local)
		if err != nil {
			return nil, fmt.Errorf("локальный адрес %q: %w", local, err)
		}
	}
	conn, err := net.DialUDP("udp", laddr, raddr)
	if err != nil {
		if laddr != nil {
			return nil, fmt.Errorf("не удалось занять локальный порт %s: %w", local, err)
		}
		return nil, err
	}
	return &Link{conn: conn, connected: true, buf: make([]byte, 65535)}, nil
}

// Listen открывает канал, принимающий датаграммы от любых узлов.
func Listen(bind string) (*Link, error) {
	addr, err := net.ResolveUDPAddr("udp", bind)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &Link{conn: conn, buf: make([]byte, 65535)}, nil
}

// FromConn оборачивает готовый сокет — используется в тестах.
func FromConn(conn *net.UDPConn, connected bool) *Link {
	return &Link{conn: conn, connected: connected, buf: make([]byte, 65535)}
}

func (l *Link) Send(payload []byte, to port.Addr) error {
	if l.connected {
		_, err := l.conn.Write(payload)
		return err
	}
	addr, ok := to.(*net.UDPAddr)
	if !ok {
		return fmt.Errorf("udplink: непригодный адрес назначения %T", to)
	}
	_, err := l.conn.WriteToUDP(payload, addr)
	return err
}

func (l *Link) Recv(timeout time.Duration) (port.Datagram, error) {
	if err := l.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return port.Datagram{}, err
	}
	n, peer, err := l.conn.ReadFromUDP(l.buf)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return port.Datagram{}, port.ErrTimeout
		}
		if errors.Is(err, net.ErrClosed) {
			return port.Datagram{}, port.ErrClosed
		}
		return port.Datagram{}, err
	}
	return port.Datagram{Payload: l.buf[:n], Peer: peer}, nil
}

func (l *Link) LocalAddr() port.Addr { return l.conn.LocalAddr() }

func (l *Link) Close() error { return l.conn.Close() }
