package jobscheduler

import (
	"context"
	"fmt"
	"github.com/tangvis/job-scheduler/dag"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func Fib(n int) int {
	if n < 2 {
		return n
	}

	return Fib(n-1) + Fib(n-2)
}

func improvedFib(n int) int {
	a, b := 0, 1
	for i := 0; i < n; i++ {
		a, b = b, a+b
	}
	return a
}

func simBiz() {
	sl := make([]int64, 10) // 模拟内存分配
	for i := range sl {
		sl[i] = 1
	}
	slb := make([]int64, len(sl))
	for idx, v := range sl {
		slb[idx] = v * rand.Int63n(100) // 模拟内存拷贝，加上随机防止编译过程优化，直接拷贝
	}
	Fib(5) // 模拟cpu运算
}

func sleep() {
	time.Sleep(10 * time.Millisecond) // 模拟io等待
}

var BizFuncCollection = map[string]Invoker{
	"p1": func(giga GigaI) error {
		sleep()
		simBiz()
		return nil
	},
	"p2": func(giga GigaI) error {
		sleep()
		simBiz()
		return nil
	},
	"p3": func(giga GigaI) error {
		sleep()
		simBiz()
		return nil
	},
	"p4": func(giga GigaI) error {
		sleep()
		simBiz()
		return nil
	},
	"p5": func(giga GigaI) error {
		sleep()
		simBiz()
		return nil
	},
	"p6": func(giga GigaI) error {
		sleep()
		simBiz()
		return nil
	},
}

var (
	jobConfig1 = ConfigEntity{
		ID:        1,
		FuncGroup: []string{"p1"},
		IgnoreLog: true,
	}
	jobConfig2 = ConfigEntity{
		ID:        2,
		FuncGroup: []string{"p2"},
		IgnoreLog: true,
	}
	jobConfig3 = ConfigEntity{
		ID:        3,
		FuncGroup: []string{"p3"},
		IgnoreLog: true,
	}
	jobConfig4 = ConfigEntity{
		ID:        4,
		FuncGroup: []string{"p4"},
		IgnoreLog: true,
	}
	jobConfig5 = ConfigEntity{
		ID:        5,
		FuncGroup: []string{"p5"},
		IgnoreLog: true,
	}
	jobConfig6 = ConfigEntity{
		ID:        6,
		FuncGroup: []string{"p6"},
		IgnoreLog: true,
	}

	mainConfig = JobConfig{
		MaxWorker: 0,
		Timeout:   1000,
		Configs:   nil,
	}
)

// TestGiga_Start
// P1───►P2
// │
// └──►P3
func TestGiga_Start(t *testing.T) {
	d := dag.NewDAG()
	v1 := dag.NewVertex(1, jobConfig1)
	v2 := dag.NewVertex(2, jobConfig2)
	v3 := dag.NewVertex(3, jobConfig3)
	d.AddVertex(v1)
	d.AddVertex(v2)
	d.AddVertex(v3)
	err := d.AddEdge(v1, v2)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v1, v3)
	if err != nil {
		t.Fatal(err)
	}
	giga := NewGiga(context.Background(), mainConfig)
	giga, f, err := BuildScheduleJobs(giga, d, BizFuncCollection)
	defer f()
	if err != nil {
		t.Fatal(err)
	}
	if err = RunGiga(giga); err != nil {
		t.Fatal(err)
	}
}

// TestGiga_StartCycle
// 环
// ┌───►P1───►P2
// │    │
// │    ▼
// P4◄──P3
// │
// ▼
// P5
func TestGiga_StartCycle(t *testing.T) {
	d := dag.NewDAG()
	v1 := dag.NewVertex(1, jobConfig1)
	v2 := dag.NewVertex(2, jobConfig2)
	v3 := dag.NewVertex(3, jobConfig3)
	v4 := dag.NewVertex(4, jobConfig4)
	v5 := dag.NewVertex(5, jobConfig5)
	d.AddVertex(v1)
	d.AddVertex(v2)
	d.AddVertex(v3)
	d.AddVertex(v4)
	d.AddVertex(v5)
	err := d.AddEdge(v1, v2)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v1, v3)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v3, v4)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v4, v5)
	if err != nil {
		t.Fatal(err)
	}
	// 构造环
	err = d.AddEdge(v4, v1)
	if err != nil {
		t.Fatal(err)
	}
	giga := NewGiga(context.Background(), mainConfig)
	giga, f, err := BuildScheduleJobs(giga, d, BizFuncCollection)
	defer f()
	if err != DagHasCycleErr {
		t.Fatal(err)
	}
}

// TestGiga_StartCycle
// 非连通图，有环
// P1──►P2
//
// P3──►P4
// ▲    │
// │    ▼
// └────P5
func TestGiga_StartCycle1(t *testing.T) {
	d := dag.NewDAG()
	v1 := dag.NewVertex(1, jobConfig1)
	v2 := dag.NewVertex(2, jobConfig2)
	v3 := dag.NewVertex(3, jobConfig3)
	v4 := dag.NewVertex(4, jobConfig4)
	v5 := dag.NewVertex(5, jobConfig5)
	d.AddVertex(v1)
	d.AddVertex(v2)
	d.AddVertex(v3)
	d.AddVertex(v4)
	d.AddVertex(v5)
	err := d.AddEdge(v1, v2)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v3, v4)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v4, v5)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v5, v3)
	if err != nil {
		t.Fatal(err)
	}
	giga := NewGiga(context.Background(), mainConfig)
	giga, f, err := BuildScheduleJobs(giga, d, BizFuncCollection)
	defer f()
	if err != DagHasCycleErr {
		t.Fatal(err)
	}
}

// TestGiga_StartCycle
// 连通图
// P1──►P2◄──┐
//
//	│
//	│
//
// P3──►P4   │
//
//	│    │
//	▼    │
//	P5───┘
func TestGiga_StartNormal(t *testing.T) {
	d := dag.NewDAG()
	v1 := dag.NewVertex(1, jobConfig1)
	v2 := dag.NewVertex(2, jobConfig2)
	v3 := dag.NewVertex(3, jobConfig3)
	v4 := dag.NewVertex(4, jobConfig4)
	v5 := dag.NewVertex(5, jobConfig5)
	d.AddVertex(v1)
	d.AddVertex(v2)
	d.AddVertex(v3)
	d.AddVertex(v4)
	d.AddVertex(v5)
	err := d.AddEdge(v1, v2)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v3, v4)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v4, v5)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v5, v2)
	if err != nil {
		t.Fatal(err)
	}
	giga := NewGiga(context.Background(), mainConfig)
	giga, f, err := BuildScheduleJobs(giga, d, BizFuncCollection)
	defer f()
	if err != nil {
		t.Fatal(err)
	}
	if err = RunGiga(giga); err != nil {
		t.Fatal(err)
	}
}

// TestGiga_StartSplit
// 非连通图
// P1──►P2
//
// P3──►P4
func TestGiga_StartSplit(t *testing.T) {
	d := dag.NewDAG()
	v1 := dag.NewVertex(1, jobConfig1)
	v2 := dag.NewVertex(2, jobConfig2)
	v3 := dag.NewVertex(3, jobConfig3)
	v4 := dag.NewVertex(4, jobConfig4)
	d.AddVertex(v1)
	d.AddVertex(v2)
	d.AddVertex(v3)
	d.AddVertex(v4)
	err := d.AddEdge(v1, v2)
	if err != nil {
		t.Fatal(err)
	}
	err = d.AddEdge(v3, v4)
	if err != nil {
		t.Fatal(err)
	}
	giga := NewGiga(context.Background(), mainConfig)
	giga, f, err := BuildScheduleJobs(giga, d, BizFuncCollection)
	defer f()
	if err != nil {
		t.Fatal(err)
	}
	if err = RunGiga(giga); err != nil {
		t.Fatal(err)
	}
}

func complexF() error {
	d := dag.NewDAG()
	v1 := dag.NewVertex(1, jobConfig1)
	v2 := dag.NewVertex(2, ConfigEntity{
		FuncGroup: []string{"p1"},
		IgnoreLog: true,
	})
	v3 := dag.NewVertex(3, jobConfig3)
	v4 := dag.NewVertex(4, jobConfig4)
	v5 := dag.NewVertex(5, jobConfig5)
	v6 := dag.NewVertex(6, jobConfig6)
	d.AddVertex(v1)
	d.AddVertex(v2)
	d.AddVertex(v3)
	d.AddVertex(v4)
	d.AddVertex(v5)
	d.AddVertex(v6)
	err := d.AddEdge(v1, v2)
	if err != nil {
		return err
	}
	err = d.AddEdge(v1, v3)
	if err != nil {
		return err
	}
	err = d.AddEdge(v1, v4)
	if err != nil {
		return err
	}
	err = d.AddEdge(v1, v5)
	if err != nil {
		return err
	}
	err = d.AddEdge(v2, v6)
	if err != nil {
		return err
	}
	err = d.AddEdge(v3, v6)
	if err != nil {
		return err
	}
	err = d.AddEdge(v4, v6)
	if err != nil {
		return err
	}
	err = d.AddEdge(v5, v6)
	if err != nil {
		return err
	}
	giga := NewGiga(context.Background(), mainConfig)
	giga, f, err := BuildScheduleJobs(giga, d, BizFuncCollection)
	defer f()
	if err != nil {
		return err
	}
	return RunGiga(giga)
}

// TestGiga_StartComplex
// P1     P2     P3
// │     │  │     │
// │     │  │     │
// └─►P4◄┘◄-└►P5◄─┘
//
//	│
//	▼
//	P6
func TestGiga_StartComplex(t *testing.T) {
	if err := complexF(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSimBiz(b *testing.B) {
	b.ResetTimer()
	b.StopTimer()
	b.StartTimer() //重新开始时间
	for n := 0; n < b.N; n++ {
		if n%100 == 0 {
			fmt.Printf("协程数量->%d\n", runtime.NumGoroutine())
		}
		simBiz()
	}
}

// BenchmarkGiga_StartComplex
// P1     P2     P3
// │     │  │     │
// │     │  │     │
// └─►P4◄┘◄-└►P5◄─┘
//
//	│
//	▼
//	P6
func BenchmarkGiga_StartComplex(b *testing.B) {
	b.ResetTimer()
	b.StopTimer()
	b.StartTimer() //重新开始时间
	//for n := 0; n < b.N; n++ {
	//	if n%100 == 0 {
	//		fmt.Printf("协程数量->%d\n", runtime.NumGoroutine())
	//	}
	//	_ = complexF()
	//}
	var n int
	b.SetParallelism(300) // 基准测试的启动并发协程数
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if n%10000 == 0 {
				fmt.Printf("协程数量->%d\n", runtime.NumGoroutine())
			}
			n++
			_ = complexF()
			time.Sleep(1 * time.Millisecond)
		}
	})
}
