package jobscheduler

import (
	"sync"
)

type Resource struct {
	ProcessorPool sync.Pool
}

var ResourcePool Resource

func init() {
	ResourcePool = Resource{}
	ResourcePool.ProcessorPool.New = func() interface{} {
		return NewProcessor(nil, 0, "")
	}
}
