// Package udplink реализует port.Link поверх UDP-сокета.
package udplink

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/argus/udpr/internal/port"
)

// maxPeers ограничивает кеш адресов источников. Слушающий канал видит адреса
// кого угодно, и без предела кеш стал бы способом раздуть память чужим
// трафиком. При переполнении кеш просто сбрасывается.
const maxPeers = 4096

// peerAddr — адрес источника с заранее посчитанным текстовым видом.
// Прикладной слой строит по нему ключ сессии на каждый пакет, а
// (*net.UDPAddr).String() — это форматирование и три аллокации на вызов.
type peerAddr struct {
	ap  netip.AddrPort
	str string
}

func (a *peerAddr) String() string { return a.str }

var _ port.Link = (*Link)(nil)

// Link — канал передачи датаграмм поверх UDP.
//
// Адреса источников кешируются: один и тот же отправитель шлёт тысячи пакетов,
// и текстовый вид его адреса считается один раз.
//
// Recv отдаёт срез общего буфера чтения, поэтому вызывать его параллельно
// из нескольких горутин нельзя: читатель у канала ровно один. Send
// потокобезопасен — за ним стоит один вызов ядра.
type Link struct {
	conn      *net.UDPConn
	connected bool // сокет привязан к одному узлу (Dial)
	buf       []byte
	peers     map[netip.AddrPort]*peerAddr
}

// newLink собирает канал поверх готового сокета.
func newLink(conn *net.UDPConn, connected bool) *Link {
	return &Link{
		conn:      conn,
		connected: connected,
		buf:       make([]byte, 65535),
		peers:     make(map[netip.AddrPort]*peerAddr),
	}
}

// peer отдаёт кешированный адрес источника, заводя запись при первой встрече.
func (l *Link) peer(ap netip.AddrPort) *peerAddr {
	// ReadFromUDPAddrPort отдаёт IPv4 в виде 4-in-6; без Unmap адрес печатался
	// бы как ::ffff:10.0.0.1 и не совпал бы с тем, что задал оператор
	ap = netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	if a, ok := l.peers[ap]; ok {
		return a
	}
	if len(l.peers) >= maxPeers {
		clear(l.peers)
	}
	a := &peerAddr{ap: ap, str: ap.String()}
	l.peers[ap] = a
	return a
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
	return newLink(conn, true), nil
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
	return newLink(conn, false), nil
}

// FromConn оборачивает готовый сокет — используется в тестах.
func FromConn(conn *net.UDPConn, connected bool) *Link {
	return newLink(conn, connected)
}

func (l *Link) Send(payload []byte, to port.Addr) error {
	if l.connected {
		_, err := l.conn.Write(payload)
		return err
	}
	addr, ok := to.(*peerAddr)
	if !ok {
		return fmt.Errorf("udplink: непригодный адрес назначения %T", to)
	}
	_, err := l.conn.WriteToUDPAddrPort(payload, addr.ap)
	return err
}

func (l *Link) Recv(timeout time.Duration) (port.Datagram, error) {
	if err := l.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return port.Datagram{}, err
	}
	n, ap, err := l.conn.ReadFromUDPAddrPort(l.buf)
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
	return port.Datagram{Payload: l.buf[:n], Peer: l.peer(ap)}, nil
}

func (l *Link) LocalAddr() port.Addr { return l.conn.LocalAddr() }

func (l *Link) Close() error { return l.conn.Close() }
