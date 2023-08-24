package jobscheduler

import (
	"time"
)

type JobConfig struct {
	MaxWorker int64          `json:"max_worker"`
	Timeout   time.Duration  `json:"timeout"`
	Configs   []ConfigEntity `json:"configs"`
}

type ConfigEntity struct {
	ID int `json:"id"`

	Prev []int `json:"prev"`

	IgnoreRetry      bool `json:"ignore_retry"`
	IgnoreCat        bool `json:"ignore_cat"`
	IgnoreLog        bool `json:"ignore_log"`
	IgnorePrometheus bool `json:"ignore_prometheus"`

	FuncGroup []string `json:"job_group"`
}

func RemoveConfig(config JobConfig, excludeIDs []int) JobConfig {
	ret := make([]ConfigEntity, 0)
	for _, configEntity := range config.Configs {
		if IntContains(excludeIDs, configEntity.ID) {
			continue
		}
		ret = append(ret, configEntity)
	}
	config.Configs = ret
	return config
}
