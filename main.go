package main

import (
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type PaymentRequest struct {
	CorrelationID string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
	Fallback      bool
}

var (
	saveChan     = make(chan PaymentRequest, 10000)
	storage      = make([]PaymentRequest, 10000)
	storageMutex sync.RWMutex
	isMaster     = false
)

func main() {
	numWorkers := getenvInt("NUM_WORKERS", 10)
	for i := 1; i <= numWorkers; i++ {
		go worker()
	}

	if os.Getenv("MASTER") == "true" {
		isMaster = true
	}
	if isMaster {
		fmt.Println("Starting circuit controller")
		go circuitController()
	}

	go startInMemorySaver()

	app := fiber.New()
	app.Post("/circuit/:status", func(c *fiber.Ctx) error {
		status, err := c.ParamsInt("status", 0)
		if err != nil {
			status = 0
		}

		atomic.StoreInt32(&circuitStatusFlag, int32(status))

		return nil
	})
	app.Post("/payments", func(c *fiber.Ctx) error {
		var paymentRequest PaymentRequest
		if err := c.BodyParser(&paymentRequest); err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		queue <- &paymentRequest
		return c.SendStatus(fiber.StatusCreated)
	})
	app.Get("/payments-summary-random", func(c *fiber.Ctx) error {
		fromStr := c.Query("from")
		toStr := c.Query("to")

		from, err := time.Parse("2006-01-02T15:04:05.000Z", fromStr)
		if err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		to, err := time.Parse("2006-01-02T15:04:05.000Z", toStr)
		if err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		var ids []string
		for _, p := range storage {
			if p.RequestedAt.Before(from) || p.RequestedAt.After(to) {
				continue
			}
			ids = append(ids, p.CorrelationID)
		}
		return c.Status(fiber.StatusOK).JSON(ids)
	})
	app.Get("/payments/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")

		for _, paymentRequest := range storage {
			if paymentRequest.CorrelationID == id {
				return c.Status(fiber.StatusOK).JSON(paymentRequest)
			}
		}
		return c.SendStatus(fiber.StatusNotFound)
	})
	app.Get("/payments-summary", func(c *fiber.Ctx) error {
		fromStr := c.Query("from")
		toStr := c.Query("to")
		internal := c.Query("internal", "false")

		var from, to time.Time
		var err error
		useDateFilter := false

		if fromStr != "" && toStr != "" {
			from, err = time.Parse("2006-01-02T15:04:05.000Z", fromStr)
			if err != nil {
				return c.SendStatus(fiber.StatusBadRequest)
			}

			to, err = time.Parse("2006-01-02T15:04:05.000Z", toStr)
			if err != nil {
				return c.SendStatus(fiber.StatusBadRequest)
			}

			useDateFilter = true
		}

		fmt.Printf("[%s][SUMMARY-REQUEST] Receiving request with from: %v and %v\n",
			getUTCNowFormatted(), formatDate(from), formatDate(to))

		var defCount, fbCount int
		var defTotal, fbTotal float64

		if !isMaster && internal != "true" {
			defCount, defTotal, fbCount, fbTotal = fetchPaymentSummary(from, to, useDateFilter, false, masterURL)
		} else {
			defCount, defTotal, fbCount, fbTotal = getPaymentSummary(from, to, useDateFilter)
			if internal != "true" {
				for _, slaveURL := range slavesURL {
					defCountIntern, defTotalIntern, fbCountIntern, fbTotalIntern := fetchPaymentSummary(from, to, useDateFilter, true, slaveURL)
					defCount += defCountIntern
					defTotal += defTotalIntern
					fbCount += fbCountIntern
					fbTotal += fbTotalIntern
				}
			}
		}

		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"default": fiber.Map{
				"totalRequests": defCount,
				"totalAmount":   defTotal,
			},
			"fallback": fiber.Map{
				"totalRequests": fbCount,
				"totalAmount":   fbTotal,
			},
		})
	})

	log.Fatal(app.Listen(":8080"))
}

func getPaymentSummary(from, to time.Time, useDateFilter bool) (
	defaultCount int, defaultAmount float64, fallbackCount int, fallbackAmount float64) {
	storageMutex.RLock()
	defer storageMutex.RUnlock()

	for _, p := range storage {
		if useDateFilter {
			if p.RequestedAt.Before(from) || p.RequestedAt.After(to) {
				continue
			}
		}

		if p.Fallback {
			fallbackCount++
			fallbackAmount += p.Amount
		} else {
			defaultCount++
			defaultAmount += p.Amount
		}
	}

	return
}

func fetchPaymentSummary(from, to time.Time, useDateFilter, internal bool, baseUrl string) (defCount int, defTotal float64, fbCount int, fbTotal float64) {
	endpoint, err := url.Parse(baseUrl + "/payments-summary")
	if err != nil {
		fmt.Println(fmt.Errorf("erro ao montar URL: %w", err))
		return 0, 0, 0, 0
	}

	q := endpoint.Query()
	if useDateFilter {
		q.Set("from", from.Format("2006-01-02T15:04:05.000Z"))
		q.Set("to", to.Format("2006-01-02T15:04:05.000Z"))
	}
	if internal {
		q.Set("internal", "true")
	}
	endpoint.RawQuery = q.Encode()

	resp, err := circuitClient.Get(endpoint.String())
	if err != nil {
		fmt.Println(fmt.Errorf("erro na requisição: %w", err))
		return 0, 0, 0, 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println(fmt.Errorf("erro HTTP: status %s", resp.Status))
		return 0, 0, 0, 0
	}

	var parsed struct {
		Default struct {
			TotalRequests int     `json:"totalRequests"`
			TotalAmount   float64 `json:"totalAmount"`
		} `json:"default"`
		Fallback struct {
			TotalRequests int     `json:"totalRequests"`
			TotalAmount   float64 `json:"totalAmount"`
		} `json:"fallback"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		fmt.Println(fmt.Errorf("erro ao decodificar JSON: %w", err))
		return 0, 0, 0, 0
	}

	return parsed.Default.TotalRequests, parsed.Default.TotalAmount,
		parsed.Fallback.TotalRequests, parsed.Fallback.TotalAmount
}

func getUTCNowFormatted() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

func formatDate(time time.Time) string {
	return time.Format("2006-01-02T15:04:05.000Z")
}
