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
			time.Sleep(500 * time.Millisecond)
			continue
		}

		targetURL := primaryURL
		if status == 1 {
			targetURL = fallbackURL
		}
		targetURL += "/payments"

		payload, _ := json.Marshal(p)
		payloadBuffer := bytes.NewBuffer(payload)
		req, _ := http.NewRequest("POST", targetURL, payloadBuffer)
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		resp, err := workerClient.Do(req)
		duration := time.Since(start).Milliseconds()

		if resp != nil {
			resp.Body.Close()
		}

		if isMaster {
			metricsChan <- Metric{
				Primary:    status != 2,
				DurationMs: duration,
				Failed:     err != nil,
			}
		}

		if err != nil && status != 1 {
			fallbackReq, _ := http.NewRequest("POST", fallbackURL+"/payments", payloadBuffer)
			fallbackReq.Header.Set("Content-Type", "application/json")
			if fbResp, fbErr := workerClient.Do(fallbackReq); fbErr == nil && fbResp != nil {
				fbResp.Body.Close()
				p.Fallback = true
			} else {
				if isMaster {
					metricsChan <- Metric{
						Primary:    false,
						DurationMs: 1000,
						Failed:     true,
					}
				}
				queue <- p
				continue
			}
		}

		saveChan <- *p
	}
}

func startInMemorySaver() {
	for msg := range saveChan {
		storageMutex.Lock()
		storage = append(storage, msg)
		storageMutex.Unlock()
	}
}
