package main

import (
	"fmt"
	"sync"
	"time"
)

// Data represents the data to be processed.
type Data struct {
	ID   int
	Info string
}

// Consumer represents a single consumer instance.
type Consumer struct {
	ID        int
	DataChan  chan Data
}

// Start processing data from its channel.
func (c *Consumer) Start(wg *sync.WaitGroup) {
	go func() {
		// Dừng xử lý khi kênh dữ liệu đóng.
		for d := range c.DataChan {
			// Giảm WaitGroup khi xử lý xong một phần dữ liệu.
			wg.Done()

			// Simulate processing time.
			time.Sleep(time.Duration(d.ID) * 100 * time.Millisecond)
			fmt.Printf("Consumer #%d đã xử lý xong dữ liệu: ID=%d, Info=%s\n", c.ID, d.ID, d.Info)
		}
	}()
}

func main() {
	const numConsumers = 3
	const numDataItems = 5

	// 1. Tạo kênh nguồn nơi dữ liệu được tạo ra.
	sourceChan := make(chan Data)

	// 2. Tạo các consumer và các kênh riêng cho mỗi consumer.
	consumers := make([]*Consumer, numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumers[i] = &Consumer{
			ID:        i + 1,
			DataChan:  make(chan Data, numDataItems),
		}
		consumers[i].Start(nil) // Khởi động goroutine xử lý cho mỗi consumer.
	}

	// 3. Goroutine phân phối dữ liệu từ nguồn đến các consumer.
	go func() {
		defer close(sourceChan)
		for i := 1; i <= numDataItems; i++ {
			sourceChan <- Data{ID: i, Info: fmt.Sprintf("Data-%d", i)}
		}
	}()
	
	fmt.Println("Bắt đầu phân phối dữ liệu từ kênh nguồn...")

	// Vòng lặp chính để phân phối dữ liệu từ nguồn đến các consumer và đợi chúng.
	for d := range sourceChan {
		var wg sync.WaitGroup
		
		// Tăng WaitGroup cho mỗi consumer.
		wg.Add(numConsumers)
		
		fmt.Printf("\nPhân phối dữ liệu ID=%d cho %d consumer...\n", d.ID, numConsumers)
		
		// Phân phối dữ liệu tới từng consumer.
		for _, consumer := range consumers {
			consumer.DataChan <- d
		}
		
		// Đợi tất cả consumer hoàn thành xử lý dữ liệu hiện tại.
		wg.Wait()
		
		fmt.Printf("=> Tất cả consumer đã hoàn thành cho dữ liệu ID=%d.\n", d.ID)
	}

	fmt.Println("\nHoàn thành xử lý tất cả dữ liệu.")
}