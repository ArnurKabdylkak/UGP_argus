package udpr

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math/big"
	mrand "math/rand"
	"net"
	"time"
)

// SenderConfig — параметры отправителя.
type SenderConfig struct {
	MTU        int           // payload на пакет, байт
	Window     int           // размер окна в пакетах (<= BitmapBits)
	RTO        time.Duration // таймаут повторной отправки
	MaxRetries int           // предел попыток на пакет
	Loss       float64       // имитация потерь канала, 0..1
	AckRate    float64       // предел частоты ACK, шт/с (0 — по умолчанию)
	AckBurst   float64       // допустимый всплеск ACK (0 — по умолчанию)
	Session    uint32        // 0 — сгенерировать случайно
	// LocalAddr фиксирует локальный адрес и порт отправителя. Без него ОС
	// выдаёт эфемерный порт, который меняется при каждом запуске, и обратный
	// ACK-канал невозможно описать одним постоянным правилом на межсетевом
	// экране. Формат "host:port" либо ":port".
	LocalAddr string
	Verbose   bool
}

// DefaultSenderConfig — разумные значения по умолчанию для MVP.
func DefaultSenderConfig() SenderConfig {
	return SenderConfig{
		MTU:        MaxPayload,
		Window:     32,
		RTO:        250 * time.Millisecond,
		MaxRetries: 20,
		// приёмник агрегирует ACK, поэтому легальная частота — это pps/AckEvery
		// плюс немедленные ACK на разрывы; 20k/с с запасом покрывает 10 Гбит/с
		AckRate:  20000,
		AckBurst: 1024,
	}
}

// SenderStats — счётчики по итогам передачи.
type SenderStats struct {
	Sent       uint64
	Retransmit uint64
	Acks       uint64
	DroppedTx  uint64
	Bytes      uint64
}

func (s SenderStats) String() string {
	return fmt.Sprintf("sent=%d retransmit=%d acks=%d dropped_tx=%d bytes=%d",
		s.Sent, s.Retransmit, s.Acks, s.DroppedTx, s.Bytes)
}

type pending struct {
	wire   []byte
	sentAt time.Time
	tries  int
	dupGap int
}

// Sender передаёт поток байт по UDPR: буферизует пакеты до ACK, следит за
// таймаутами и выполняет повторную отправку.
type Sender struct {
	cfg   SenderConfig
	conn  *net.UDPConn
	guard *AckGuard
	rng   *mrand.Rand

	session  uint32
	base     uint32 // первый неподтверждённый seq
	nextSeq  uint32
	pendings map[uint32]*pending
	stats    SenderStats
}

// NewSender открывает UDP-сокет в сторону dst. Если задан cfg.LocalAddr,
// сокет привязывается к нему: обратный ACK-канал получает постоянный
// и проверяемый 5-tuple.
func NewSender(dst string, cfg SenderConfig) (*Sender, error) {
	addr, err := net.ResolveUDPAddr("udp", dst)
	if err != nil {
		return nil, err
	}
	var laddr *net.UDPAddr
	if cfg.LocalAddr != "" {
		laddr, err = net.ResolveUDPAddr("udp", cfg.LocalAddr)
		if err != nil {
			return nil, fmt.Errorf("локальный адрес %q: %w", cfg.LocalAddr, err)
		}
	}
	conn, err := net.DialUDP("udp", laddr, addr)
	if err != nil {
		if laddr != nil {
			return nil, fmt.Errorf("не удалось занять локальный порт %s: %w", cfg.LocalAddr, err)
		}
		return nil, err
	}
	return NewSenderConn(conn, cfg), nil
}

// LocalAddr возвращает фактический локальный адрес сокета — тот, на который
// приёмник будет слать ACK.
func (s *Sender) LocalAddr() net.Addr { return s.conn.LocalAddr() }

// NewSenderConn собирает отправителя поверх готового соединения (для тестов).
func NewSenderConn(conn *net.UDPConn, cfg SenderConfig) *Sender {
	if cfg.MTU <= 0 || cfg.MTU > MaxPayload {
		cfg.MTU = MaxPayload
	}
	if cfg.Window <= 0 || cfg.Window > BitmapBits {
		cfg.Window = BitmapBits
	}
	if cfg.RTO <= 0 {
		cfg.RTO = 250 * time.Millisecond
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 20
	}
	if cfg.AckRate <= 0 {
		cfg.AckRate = 20000
	}
	if cfg.AckBurst <= 0 {
		cfg.AckBurst = 1024
	}
	session := cfg.Session
	if session == 0 {
		session = randomSession()
	}
	return &Sender{
		cfg:      cfg,
		conn:     conn,
		guard:    NewAckGuard(session, cfg.AckRate, cfg.AckBurst),
		rng:      mrand.New(mrand.NewSource(time.Now().UnixNano())),
		session:  session,
		pendings: make(map[uint32]*pending),
	}
}

// Close закрывает сокет.
func (s *Sender) Close() error { return s.conn.Close() }

// Guard даёт доступ к политике обратного канала.
func (s *Sender) Guard() *AckGuard { return s.guard }

// Session возвращает идентификатор сессии.
func (s *Sender) Session() uint32 { return s.session }

func (s *Sender) logf(format string, a ...any) {
	if s.cfg.Verbose {
		log.Printf("[sender] "+format, a...)
	}
}

func (s *Sender) wireSend(b []byte) {
	if s.cfg.Loss > 0 && s.rng.Float64() < s.cfg.Loss {
		s.stats.DroppedTx++
		return
	}
	if _, err := s.conn.Write(b); err != nil {
		s.logf("ошибка отправки: %v", err)
	}
}

func (s *Sender) emit(seq uint32) {
	p := s.pendings[seq]
	p.sentAt = time.Now()
	p.tries++
	p.dupGap = 0
	if p.tries > 1 {
		s.stats.Retransmit++
		s.logf("retransmit seq=%d try=%d", seq, p.tries)
	}
	s.wireSend(p.wire)
}

// SendStream заворачивает поток r в UDPR-пакеты и доставляет их с гарантией.
func (s *Sender) SendStream(r io.Reader) (SenderStats, error) {
	hello := &Packet{Type: TypeHello, Session: s.session, Window: uint16(s.cfg.Window)}
	if wire, err := hello.Encode(); err == nil {
		s.wireSend(wire)
	}

	acks := make(chan *Packet, 256)
	done := make(chan struct{})
	go s.readAcks(acks, done)
	defer close(done)

	ticker := time.NewTicker(s.cfg.RTO / 4)
	defer ticker.Stop()

	buf := make([]byte, s.cfg.MTU)
	eof := false
	for !eof || len(s.pendings) > 0 {
		// окно считается от base, а не от числа буферизованных пакетов:
		// иначе выборочно подтверждённые seq позволили бы уйти за окно приёма
		for !eof && s.nextSeq-s.base < uint32(s.cfg.Window) {
			n, err := io.ReadFull(r, buf)
			if n > 0 {
				if err := s.queue(buf[:n]); err != nil {
					return s.stats, err
				}
			}
			if err != nil { // EOF или ErrUnexpectedEOF — поток закончился
				eof = true
			}
		}

		select {
		case ack := <-acks:
			s.applyAck(ack)
			// добираем то, что уже пришло, не дожидаясь тика
			for drained := true; drained; {
				select {
				case a := <-acks:
					s.applyAck(a)
				default:
					drained = false
				}
			}
		case <-ticker.C:
			if err := s.checkTimeouts(); err != nil {
				return s.stats, err
			}
		}
	}

	fin := &Packet{Type: TypeFin, Session: s.session, Seq: s.nextSeq, Window: uint16(s.cfg.Window)}
	if wire, err := fin.Encode(); err == nil {
		for i := 0; i < 3; i++ {
			s.wireSend(wire)
		}
	}
	return s.stats, nil
}

func (s *Sender) queue(chunk []byte) error {
	p := &Packet{Type: TypeData, Session: s.session, Seq: s.nextSeq,
		Window: uint16(s.cfg.Window), Payload: chunk}
	wire, err := p.Encode()
	if err != nil {
		return err
	}
	s.pendings[p.Seq] = &pending{wire: wire}
	s.nextSeq++
	s.stats.Sent++
	s.stats.Bytes += uint64(len(chunk))
	s.emit(p.Seq)
	return nil
}

func (s *Sender) readAcks(out chan<- *Packet, done <-chan struct{}) {
	buf := make([]byte, 2048)
	for {
		select {
		case <-done:
			return
		default:
		}
		_ = s.conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		n, err := s.conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		p, err := Decode(buf[:n])
		if err != nil {
			s.guard.Drop(err.Error())
			continue
		}
		select {
		case out <- p:
		case <-done:
			return
		}
	}
}

// applyAck подтверждает пакеты по ack_base и bitmap. ack_base — первый ещё
// НЕ полученный seq, бит i bitmap соответствует seq = ack_base+1+i.
func (s *Sender) applyAck(ack *Packet) {
	if !s.guard.Check(ack, s.base, s.nextSeq) {
		_, reason := s.guard.Stats()
		s.logf("ACK Guard отклонил: %s", reason)
		return
	}
	s.stats.Acks++

	for seq := range s.pendings {
		if seq < ack.AckBase {
			delete(s.pendings, seq)
		}
	}
	if ack.AckBase > s.base {
		s.base = ack.AckBase
	}

	var holes []uint32
	var top uint32
	haveTop := false
	for i := 0; i < BitmapBits; i++ {
		seq := ack.AckBase + 1 + uint32(i)
		if ack.AckBitmap>>uint(i)&1 == 1 {
			delete(s.pendings, seq)
			top, haveTop = seq, true
		} else if _, ok := s.pendings[seq]; ok {
			holes = append(holes, seq)
		}
	}
	if !haveTop {
		return
	}
	// fast retransmit: дырка ниже уже подтверждённого пакета
	for _, seq := range holes {
		if seq >= top {
			continue
		}
		p := s.pendings[seq]
		p.dupGap++
		// не чаще одного fast retransmit за половину RTO, иначе поток
		// дубликатов ACK разгоняет лишние повторы
		if p.dupGap >= 2 && time.Since(p.sentAt) >= s.cfg.RTO/2 {
			s.emit(seq)
		}
	}
}

func (s *Sender) checkTimeouts() error {
	now := time.Now()
	for seq, p := range s.pendings {
		if now.Sub(p.sentAt) < s.cfg.RTO {
			continue
		}
		if p.tries > s.cfg.MaxRetries {
			return fmt.Errorf("udpr: seq=%d не доставлен после %d попыток", seq, p.tries)
		}
		s.emit(seq)
	}
	return nil
}

func randomSession() uint32 {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<32-1))
	if err != nil {
		return uint32(time.Now().UnixNano()) | 1
	}
	return uint32(n.Uint64()) | 1
}
