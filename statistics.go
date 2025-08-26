package rmds

import (
	"sync/atomic"
	"time"
)

type Statistics struct {
	delivered          uint64
	queued             uint64
	dropped            uint64
	processed          uint64
	sentAcks           uint64
	receivedAcks       uint64
	sentKeepAlives     uint64
	receivedKeepAlives uint64
	reconnects         uint64
	startTime          time.Time
}

func NewStatistics() *Statistics {
	return &Statistics{
		startTime: time.Now(),
	}
}

func (s *Statistics) IncrementDelivered() {
	atomic.AddUint64(&s.delivered, 1)
}

func (s *Statistics) IncrementQueued(count uint64) {
	atomic.AddUint64(&s.queued, count)
}

func (s *Statistics) IncrementDropped() {
	atomic.AddUint64(&s.dropped, 1)
}

func (s *Statistics) IncrementProcessed() {
	atomic.AddUint64(&s.processed, 1)
}

func (s *Statistics) IncrementSentAcks() {
	atomic.AddUint64(&s.sentAcks, 1)
}

func (s *Statistics) IncrementReceivedAcks() {
	atomic.AddUint64(&s.receivedAcks, 1)
}

func (s *Statistics) IncrementSentKeepAlives() {
	atomic.AddUint64(&s.sentKeepAlives, 1)
}

func (s *Statistics) IncrementReceivedKeepAlives() {
	atomic.AddUint64(&s.receivedKeepAlives, 1)
}

func (s *Statistics) IncrementReconnects() {
	atomic.AddUint64(&s.reconnects, 1)
}

func (s *Statistics) GetStats() map[string]uint64 {
	return map[string]uint64{
		"delivered":           atomic.LoadUint64(&s.delivered),
		"queued":              atomic.LoadUint64(&s.queued),
		"dropped":             atomic.LoadUint64(&s.dropped),
		"processed":           atomic.LoadUint64(&s.processed),
		"sent_acks":           atomic.LoadUint64(&s.sentAcks),
		"received_acks":       atomic.LoadUint64(&s.receivedAcks),
		"sent_keepalives":     atomic.LoadUint64(&s.sentKeepAlives),
		"received_keepalives": atomic.LoadUint64(&s.receivedKeepAlives),
		"reconnects":          atomic.LoadUint64(&s.reconnects),
		"uptime_seconds":      uint64(time.Since(s.startTime).Seconds()),
	}
}
