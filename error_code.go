package jobscheduler

import (
	"fmt"
)

var (
	DagHasCycleErr     = fmt.Errorf("job dag has cycle, please check the job config")
	JobFuncNotFoundErr = func(funcName string) error {
		return fmt.Errorf("func (%s) not found", funcName)
	}
	IDDuplicatedErr      = fmt.Errorf("job config has duplicate id")
	JobConfigNotLegalErr = func(i interface{}) error {
		return fmt.Errorf("config (%v) not legal", i)
	}
	EmptyConfigErr = fmt.Errorf("empty config")
)
