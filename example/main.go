// 单机持久化函数编排运行时示例：
//
//	go run ./example -dir ./state-demo
//
// 演示：定义注册 -> 幂等提交（含并发重复投递收敛）-> 事件轨迹解释。
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	durable "github.com/h3yow/durable-function-runtime-go"
)

func main() {
	dir := flag.String("dir", "./state-demo", "local state directory")
	flag.Parse()

	reg := durable.NewRegistry()
	policy := durable.RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		Multiplier:     2,
	}
	def := durable.Definition{
		Name: "order-pipeline", Version: "1.0.0",
		Steps: []durable.Step{
			{Name: "validate", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				return in, nil
			}},
			{Name: "charge", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				return json.RawMessage(`{"charged":true}`), nil
			}},
			{Name: "ship", Timeout: time.Second, Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				return json.RawMessage(`{"shipped":true}`), nil
			}},
		},
	}
	if err := reg.Register(def); err != nil {
		log.Fatal(err)
	}

	rt, err := durable.New(context.Background(), durable.Config{
		Dir: *dir, MaxConcurrent: 4, Retry: policy,
	}, reg)
	if err != nil {
		log.Fatal(err)
	}
	defer rt.Close()

	key := "order-42"
	input := json.RawMessage(`{"order_id":42,"amount":99}`)

	// 同一幂等键并发投递 8 次，必须收敛到同一执行。
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := rt.Start(context.Background(), "order-pipeline", "1.0.0", key, input)
			if err != nil {
				log.Printf("submit %d: %v", i, err)
				return
			}
			log.Printf("submit %d -> execution=%s new=%v", i, res.ExecutionID, res.NewExecution)
		}(i)
	}
	wg.Wait()

	id := "exec-" + shaPrefix(key)
	deadline := time.Now().Add(3 * time.Second)
	var exec durable.Execution
	for time.Now().Before(deadline) {
		exec, _ = rt.GetExecution(context.Background(), id)
		if exec.Status.Terminal() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("\nfinal status: %s\n", exec.Status)
	evs, _ := rt.Events(context.Background(), id)
	fmt.Println("event trail (explainable):")
	for _, ev := range evs {
		fmt.Printf("  #%d %-14s step=%-8s reason=%s\n", ev.Seq, ev.Type, ev.Step, ev.Reason)
	}
}

func shaPrefix(s string) string {
	// 与内部执行 ID 派生规则保持一致，仅用于演示展示。
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}
