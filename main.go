package main

import (
	"fmt"
	"sync"
	"time"
)

type Data struct {
	ID   int
	Info string
}

type Consumer struct {
	ID       int
	DataChan chan Data
}

func (c *Consumer) Start(wg *sync.WaitGroup) {
	go func() {

		for d := range c.DataChan {

			wg.Done()

			time.Sleep(time.Duration(d.ID) * 100 * time.Millisecond)
			fmt.Printf("Consumer #%d đã xử lý xong dữ liệu: ID=%d, Info=%s\n", c.ID, d.ID, d.Info)
		}
	}()
}

func main() {
	const numConsumers = 3
	const numDataItems = 5

	sourceChan := make(chan Data)

	consumers := make([]*Consumer, numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumers[i] = &Consumer{
			ID:       i + 1,
			DataChan: make(chan Data, numDataItems),
		}
		consumers[i].Start(nil)
	}

	go func() {
		defer close(sourceChan)
		for i := 1; i <= numDataItems; i++ {
			sourceChan <- Data{ID: i, Info: fmt.Sprintf("Data-%d", i)}
		}
	}()

	fmt.Println("Bắt đầu phân phối dữ liệu từ kênh nguồn...")

	for d := range sourceChan {
		var wg sync.WaitGroup

		wg.Add(numConsumers)

		fmt.Printf("\nPhân phối dữ liệu ID=%d cho %d consumer...\n", d.ID, numConsumers)

		for _, consumer := range consumers {
			consumer.DataChan <- d
		}

		wg.Wait()

		fmt.Printf("=> Tất cả consumer đã hoàn thành cho dữ liệu ID=%d.\n", d.ID)
	}

	fmt.Println("\nHoàn thành xử lý tất cả dữ liệu.")
}
