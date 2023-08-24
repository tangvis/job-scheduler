package jobscheduler

import (
	"fmt"
	"time"
)

const (
	CatModuleName = "SampleScheduleJobs"
)

type InterceptorI interface {
	NewLogInterceptor() func(p *Processor)
	NewCatInterceptor() func(p *Processor)
}

var _ InterceptorI = (*Interceptor)(nil)

type Interceptor struct {
}

func NewInterceptorFactory() InterceptorI {
	return &Interceptor{}
}

func (i Interceptor) NewLogInterceptor() func(p *Processor) {
	return func(p *Processor) {
		fmt.Printf("Processor:[%s|%d] start\n", p.name, p.id)
		start := time.Now()
		defer func() {
			elapsed := float64(time.Since(start).Nanoseconds()) / 1e6
			if p.err != nil {
				fmt.Printf("Processor:[%s|%d][Latency: %.3fms] failed, err: %+v\n", p.name, p.id, elapsed, p.err)
				return
			}
			fmt.Printf("Processor:[%s|%d][Latency: %.3fms] success\n", p.name, p.id, elapsed)
		}()
		p.Next()
	}
}

func (i Interceptor) NewCatInterceptor() func(p *Processor) {
	return func(p *Processor) {
		defer func() {
			// do something
		}()
		p.Next()
	}
}
