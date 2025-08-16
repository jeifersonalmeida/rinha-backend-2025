package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	QueueCapacity = 3500
)

var (
	queue        = make(chan *PaymentRequest, QueueCapacity)
	workerClient = &http.Client{Timeout: getenvDurationMS("MAX_DEFAULT_LATENCY", 100) * 2 * time.Millisecond}
)

func worker() {
	for p := range queue {
		status := atomic.LoadInt32(&circuitStatusFlag)

		if status == 2 {
			queue <- p
			time.Sleep(500 * time.Millisecond)
			continue
		}

		success := tryProcessPayment(p, status)
		if !success && status == 0 {
			fallbackSuccess := tryProcessPayment(p, 1)
			if fallbackSuccess {
				p.Fallback = true
				saveChan <- *p
			} else {
				queue <- p
			}
		} else if success {
			saveChan <- *p
		} else {
			queue <- p
		}
	}
}

func tryProcessPayment(p *PaymentRequest, circuitStatus int32) bool {
	targetURL := primaryURL
	if circuitStatus == 1 {
		targetURL = fallbackURL
	}

	p.RequestedAt = time.Now().UTC().Truncate(time.Millisecond)
	marshaled, _ := json.Marshal(p)
	req, _ := http.NewRequest("POST", targetURL+"/payments", bytes.NewBuffer(marshaled))
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := workerClient.Do(req)
	duration := time.Since(start).Milliseconds()

	if isMaster {
		metricsChan <- Metric{
			Primary:    circuitStatus == 0,
			DurationMs: duration,
			Failed:     err != nil || (resp != nil && resp.StatusCode != 200),
		}
	}

	if err != nil {
		return false
	}

	if resp != nil {
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return false
		}
	}

	return true
}

func startInMemorySaver() {
	for msg := range saveChan {
		storageMutex.Lock()
		storage = append(storage, msg)
		storageMutex.Unlock()
	}
}
