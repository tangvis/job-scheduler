package jobscheduler

import "context"

type Giga struct {
	// base giga
	BaseGiga

	// 业务数据
	Data BizData
}

type BizData struct {
}

func (giga *Giga) BizData() interface{} {
	return giga.Data
}

func (giga *Giga) Name() string {
	return "example_giga"
}

func NewGiga(ctx context.Context, config JobConfig) GigaI {
	return &Giga{
		BaseGiga: NewBaseGiga(ctx, config),
		Data:     BizData{},
	}
}
