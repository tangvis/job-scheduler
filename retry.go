package jobscheduler

import (
	"time"
)

type RequestRetryManager func(giga GigaI, err error) bool

func NewDefaultTimeoutRetryFunc(retryTimes int, sleepDuration time.Duration) RequestRetryManager {
	t := 0
	return func(giga GigaI, err error) bool {
		if err != nil {
			if t++; t <= retryTimes {
				time.Sleep(sleepDuration)
				return true
			}
		}
		return false
	}
}
