package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Job struct {
	Name string
	Args []interface{}
	Wg   *sync.WaitGroup
}

type worker struct {
	conn  redis.Conn
	queue chan Job
}

func (w worker) Run() {
	for job := range w.queue {
		w.conn.Do(job.Name, job.Args...)
		job.Wg.Done()
	}
}

func main() {
	pool := &redis.Pool{
		// Other pool configuration not shown in this example.
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}

	queue := make(chan Job, 100)

	for i := 0; i < 50; i++ {
		w := worker{conn: pool.Get(), queue: queue}
		go w.Run()
	}

	tests := 40000

	conn := pool.Get()
	fmt.Printf("Run tikv.put %d\n", tests)
	start_at := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(tests)
	for i := 0; i < tests; i++ {
		key := "pkey-" + strconv.Itoa(i)
		value := "pvaluedata-" + strconv.Itoa(i) + "-123456789809"
		queue <- Job{
			Name: "tikv.put",
			Args: []interface{}{key, value},
			Wg:   &wg,
		}
	}
	wg.Wait()
	end_at := time.Now()
	fmt.Printf("Time: %v\n", end_at.Sub(start_at))

	fmt.Printf("Run tikv.get %d\n", tests)
	start_at = time.Now()
	wg = sync.WaitGroup{}
	wg.Add(tests)
	for i := 0; i < tests; i++ {
		key := "pkey-" + strconv.Itoa(i)
		queue <- Job{
			Name: "tikv.cget",
			Args: []interface{}{key},
			Wg:   &wg,
		}
	}
	wg.Wait()
	end_at = time.Now()
	fmt.Printf("Time: %v\n", end_at.Sub(start_at))

	fmt.Printf("Run set %d\n", tests)
	start_at = time.Now()
	for i := 0; i < tests; i++ {
		key := "key-" + strconv.Itoa(i)
		value := "valuedata-" + strconv.Itoa(i) + "-123456789809"
		conn.Do("set", key, value)
	}
	end_at = time.Now()
	fmt.Printf("Time: %v\n", end_at.Sub(start_at))

	fmt.Printf("Run get %d\n", tests)
	start_at = time.Now()
	for i := 0; i < tests; i++ {
		key := "key-" + strconv.Itoa(i)
		conn.Do("get", key)
	}
	end_at = time.Now()
	fmt.Printf("Time: %v\n", end_at.Sub(start_at))
}
