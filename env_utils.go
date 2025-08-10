package main

import (
	"os"
	"strconv"
	"time"
)

func getenvBool(key string, def bool) bool {
	env, err := strconv.ParseBool(os.Getenv(key))
	if err != nil {
		return def
	}
	return env
}

func getenvInt(key string, def int) int {
	env, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return def
	}
	return env
}

func getenvInt64(key string, def int64) int64 {
	env, err := strconv.ParseInt(os.Getenv(key), 10, 64)
	if err != nil {
		return def
	}
	return env
}

func getenvDurationMS(key string, defMS int) time.Duration {
	return time.Duration(getenvInt(key, defMS)) * time.Millisecond
}

func getenvDurationSec(key string, defSec int) time.Duration {
	return time.Duration(getenvInt(key, defSec)) * time.Second
}
