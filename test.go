package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"reflect"
	"sync"
)

type OrderBook struct {
	Bids     map[float64]float64 // Map of all bids, key->price, value->quantity
	BidMutex sync.Mutex          // Threadsafe

	Asks     map[float64]float64 // Map of all asks, key->price, value->quantity
	AskMutex sync.Mutex          // Threadsafe

	Client *redis.Client
}

func main() {
	fmt.Println("11")
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ob := OrderBook{}
	ob.Client = client
	pong, err := ob.Client.Ping().Result()
	fmt.Println(pong, err)
	client.HSet("hash", "key1", "hello1")
	hSet := client.HSet("hash", "key", "hello")
	fmt.Println(hSet)
	hExists := client.HExists("hash", "key")
	fmt.Println(hExists)
	hGet := client.HGet("hash", "key")
	fmt.Println(hGet)
	m, err := client.HGetAll("hash").Result()
	fmt.Println(err)
	fmt.Println(m)
	fmt.Println(reflect.TypeOf(client))
	fmt.Printf("hello %s %f %v", "hah", 1.1, 2.3)
}
