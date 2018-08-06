/*
 Connects to the Binance WebSocket and write orderBook to redis
*/

package main

// import "github.com/davecgh/go-spew/spew"
import "time"

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gorilla/websocket"
	"github.com/pdepip/go-binance/binance"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	MaxDepth = 100 // Size of order book
	MaxQueue = 100 // Size of message queue
)

// Message received from websocket
type State struct {
	EventType string          `json:"e"`
	EventTime int64           `json:"E"`
	Symbol    string          `json:"s"`
	UpdateId  int64           `json:"u"`
	BidDelta  []binance.Order `json:"b"`
	AskDelta  []binance.Order `json:"a"`
}

// Orderbook structure
type OrderBook struct {
	Bids     map[float64]float64 // Map of all bids, key->price, value->quantity
	BidMutex sync.Mutex          // Threadsafe

	CoinType string // coin type of orderBook such as: ethbtc...

	Asks     map[float64]float64 // Map of all asks, key->price, value->quantity
	AskMutex sync.Mutex          // Threadsafe

	Client *redis.Client // Redis client to store data to redis

	Updates chan State // Channel of all state updates
}

// to convert a float number to a string
func FloatToString(input_num float64) string {
	return strconv.FormatFloat(input_num, 'f', -1, 64)
}

func (o *OrderBook) HashKey(otype string) string {
	bidKey := fmt.Sprintf("%s-%s", o.CoinType, otype)
	return bidKey
}

func (o *OrderBook) DeleteFromRedis(otype string, price float64) {
	hashKey := o.HashKey(otype)
	hDel := o.Client.HDel(hashKey, FloatToString(price))
	if hDel.Err() != nil {
		fmt.Printf("Delete price: %f from %s failed\n", price, hashKey)
	}
}

func (o *OrderBook) StoreToRedis(otype string, price float64, quantity float64) {
	hashKey := o.HashKey(otype)
	hSet := o.Client.HSet(hashKey, FloatToString(price), FloatToString(quantity))
	if hSet.Err() != nil {
		fmt.Printf("Store price: %f and quantity: %f to redis hash key: %s failed\n", price, quantity, hashKey)
	}
}

// Process all incoming bids
func (o *OrderBook) ProcessBids(bids []binance.Order) {
	for _, bid := range bids {
		if bid.Quantity == 0 {
			o.DeleteFromRedis("bids", bid.Price)
		} else {
			o.StoreToRedis("bids", bid.Price, bid.Quantity)
		}
	}
}

// Process all incoming asks
func (o *OrderBook) ProcessAsks(asks []binance.Order) {
	for _, ask := range asks {
		if ask.Quantity == 0 {
			o.DeleteFromRedis("asks", ask.Price)
		} else {
			o.StoreToRedis("asks", ask.Price, ask.Quantity)
		}
	}
}

// Hands off incoming messages to processing functions
func (o *OrderBook) Maintainer() {
	for {
		select {
		case job := <-o.Updates:
			if len(job.BidDelta) > 0 {
				go o.ProcessBids(job.BidDelta)
			}

			if len(job.AskDelta) > 0 {
				go o.ProcessAsks(job.AskDelta)
			}
		}
	}
}

func GetEnv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func FetchOrderBook(symbol string) {
	db, _ := strconv.Atoi(GetEnv("REDIS_DB", "0"))
	redisClient := redis.NewClient(&redis.Options{
		Addr:     GetEnv("REDIS_HOST", "localhost:6379"),
		Password: GetEnv("REDIS_PASSWORD", ""), // no password set
		DB:       db,
	})

	address := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@depth", symbol)

	// Connect to websocket
	var wsDialer = &websocket.Dialer{HandshakeTimeout: 30 * time.Second}
	wsConn, _, err := wsDialer.Dial(address, nil)
	if err != nil {
		panic(err)
	}
	defer wsConn.Close()
	log.Println("Dialed:", address)

	// Set up Order Book
	ob := OrderBook{}
	ob.Bids = make(map[float64]float64, MaxDepth)
	ob.Asks = make(map[float64]float64, MaxDepth)
	ob.Updates = make(chan State, 500)
	ob.Client = redisClient
	ob.CoinType = symbol

	// Get initial state of orderbook from rest api
	client := binance.New("", "")
	query := binance.OrderBookQuery{
		Symbol: strings.ToUpper(symbol),
	}
	orderBook, err := client.GetOrderBook(query)
	if err != nil {
		panic(err)
	}

	ob.ProcessBids(orderBook.Bids)
	ob.ProcessAsks(orderBook.Asks)

	// Start maintaining order book
	go ob.Maintainer()

	// Read & Process Messages from wss stream
	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			log.Println("[ERROR] ReadMessage:", err)
		}

		msg := State{}
		err = json.Unmarshal(message, &msg)
		if err != nil {
			log.Println("[ERROR] Parsing:", err)
			continue
		}
		ob.Updates <- msg
	}
}

func main() {
	fmt.Println("start fetching binance order book")
	FetchOrderBook("ethbtc")
}
