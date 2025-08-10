package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync/atomic"
	"time"
)

type ServiceHealth struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
	LastChecked     time.Time
}

type Metric struct {
	Primary    bool
	DurationMs int64
	Failed     bool
}

var (
	circuitClient = &http.Client{Timeout: 2 * time.Second}

	primaryURL  = "http://payment-processor-default:8080"
	fallbackURL = "http://payment-processor-fallback:8080"

	masterURL = "http://api-1:8080"
	slaveURL  = "http://api-2:8080"

	circuitStatusFlag int32 // 0 = principal, 1 = fallback, 2 = aberto

	metricsChan = make(chan Metric, 2000)

	primaryHealth  = ServiceHealth{}
	fallbackHealth = ServiceHealth{}
)

func checkHealth(baseURL string) ServiceHealth {
	resp, err := circuitClient.Get(baseURL + "/payments/service-health")
	if err != nil {
		fmt.Printf("Erro health check: %v\n", err.Error())
		return ServiceHealth{Failing: true, MinResponseTime: 1000, LastChecked: time.Now()}
	}
	defer resp.Body.Close()

	var health ServiceHealth
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		fmt.Printf("Erro health check: %v\n", err.Error())
		return ServiceHealth{Failing: true, MinResponseTime: 1000, LastChecked: time.Now()}
	}
	health.LastChecked = time.Now()
	return health
}

func circuitController() {
	useFallbackAllowed := getenvBool("USE_FALLBACK", false)
	maxDefaultLatency := getenvInt64("MAX_DEFAULT_LATENCY", 100)
	maxFallbackLatency := getenvInt64("MAX_FALLBACK_LATENCY", 100)
	sampleWindow := getenvInt("SAMPLE_WINDOW", 100)
	tickInterval := getenvDurationMS("TICK_INTERVAL_MS", 500)
	healthInterval := getenvDurationMS("HEALTH_INTERVAL_MS", 5000)
	failoverDelay := getenvDurationSec("PRIMARY_FAILOVER_DELAY_SEC", 15)

	var (
		primaryTimes          []int64
		fallbackTimes         []int64
		fallbackEligibleSince time.Time
	)

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	healthTicker := time.NewTicker(healthInterval)
	defer healthTicker.Stop()

	primaryHealth = checkHealth(primaryURL)
	fallbackHealth = checkHealth(fallbackURL)

	for {
		select {
		case m := <-metricsChan:
			if m.Primary {
				primaryTimes = append(primaryTimes, m.DurationMs)
				if len(primaryTimes) > sampleWindow {
					primaryTimes = primaryTimes[1:]
				}
			} else {
				fallbackTimes = append(fallbackTimes, m.DurationMs)
				if len(fallbackTimes) > sampleWindow {
					fallbackTimes = fallbackTimes[1:]
				}
			}

		case <-healthTicker.C:
			primaryHealth = checkHealth(primaryURL)
			primaryTimes = []int64{int64(primaryHealth.MinResponseTime)}
			fallbackHealth = checkHealth(fallbackURL)
			fallbackTimes = []int64{int64(fallbackHealth.MinResponseTime)}

		case <-ticker.C:
			now := time.Now()

			var p95Primary, p95Fallback int64
			if len(primaryTimes) > 0 {
				p95Primary = p95(primaryTimes)
			}
			if len(fallbackTimes) > 0 {
				p95Fallback = p95(fallbackTimes)
			}

			status := 0

			if primaryHealth.Failing && fallbackHealth.Failing {
				fallbackEligibleSince = time.Time{}
			} else {
				if p95Primary > 0 && p95Primary > maxDefaultLatency {
					if useFallbackAllowed && !fallbackHealth.Failing {
						if fallbackEligibleSince.IsZero() {
							fallbackEligibleSince = now
						}
						if now.Sub(fallbackEligibleSince) >= failoverDelay {
							status = 1
						} else {
							status = 2
						}
					} else {
						status = 2
						fallbackEligibleSince = time.Time{}
					}
				} else if primaryHealth.Failing {
					if useFallbackAllowed && !fallbackHealth.Failing {
						if fallbackEligibleSince.IsZero() {
							fallbackEligibleSince = now
						}
						if now.Sub(fallbackEligibleSince) >= failoverDelay {
							status = 1
						} else {
							status = 2
						}
					} else {
						status = 2
						fallbackEligibleSince = time.Time{}
					}
				} else {
					fallbackEligibleSince = time.Time{}
				}

				if status == 1 {
					if p95Fallback > 0 && p95Fallback > maxFallbackLatency {
						status = 2
						fallbackEligibleSince = time.Time{}
					}
				}
			}

			if atomic.LoadInt32(&circuitStatusFlag) != int32(status) {
				go notifySlave(status)
				atomic.StoreInt32(&circuitStatusFlag, int32(status))
			}
		}
	}
}

func p95(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	cp := make([]int64, len(values))
	copy(cp, values)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	rank := int(math.Ceil(0.95*float64(len(cp)))) - 1
	if rank < 0 {
		rank = 0
	}
	return cp[rank]
}

func notifySlave(status int) {
	url := fmt.Sprintf("%s/circuit/%d", slaveURL, status)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		fmt.Println("Erro ao criar requisição:", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := circuitClient.Do(req)
	if err != nil {
		fmt.Println("Erro ao enviar requisição:", err)
		return
	}
	defer resp.Body.Close()
}
