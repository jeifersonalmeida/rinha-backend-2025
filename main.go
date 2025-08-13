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

		paymentRequest.RequestedAt = time.Now()
		queue <- &paymentRequest
		return c.SendStatus(fiber.StatusCreated)
	})
	app.Get("/payments-summary", func(c *fiber.Ctx) error {
		fmt.Printf("[%s][SUMMARY-REQUEST] Receving a summary request...\n", time.Now().UTC().Format(time.RFC3339))

		fromStr := c.Query("from")
		toStr := c.Query("to")
		internal := c.Query("internal", "false")

		from, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		to, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		defCount, defTotal, fbCount, fbTotal := getPaymentSummary(from, to)
		if internal != "true" {
			defCountIntern, defTotalIntern, fbCountIntern, fbTotalIntern := fetchPaymentSummary(from, to)
			defCount += defCountIntern
			defTotal += defTotalIntern
			fbCount += fbCountIntern
			fbTotal += fbTotalIntern
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

func getPaymentSummary(from, to time.Time) (defaultCount int, defaultAmount float64, fallbackCount int, fallbackAmount float64) {
	storageMutex.RLock()
	defer storageMutex.RUnlock()

	for _, p := range storage {
		if (p.RequestedAt.Equal(from) || p.RequestedAt.After(from)) &&
			(p.RequestedAt.Before(to) || p.RequestedAt.Equal(to)) {
			if p.Fallback {
				fallbackCount++
				fallbackAmount += p.Amount
			} else {
				defaultCount++
				defaultAmount += p.Amount
			}
		}
	}

	return
}

func fetchPaymentSummary(from, to time.Time) (defCount int, defTotal float64, fbCount int, fbTotal float64) {
	baseURL := slaveURL
	if !isMaster {
		baseURL = masterURL
	}
	endpoint, err := url.Parse(baseURL + "/payments-summary")
	if err != nil {
		fmt.Println(fmt.Errorf("erro ao montar URL: %w", err))
		return 0, 0, 0, 0
	}

	q := endpoint.Query()
	q.Set("from", from.Format("2006-01-02T15:04:05.000Z"))
	q.Set("to", to.Format("2006-01-02T15:04:05.000Z"))
	q.Set("internal", "true")
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
