package main

import "github.com/gomodule/redigo/redis"

const (
	HOST = "127.0.0.1"
	PORT = 23000

	APP_ID     = 7
	APP_KEY    = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
	APP_SECRET = "0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1"
	URL        = "http://192.168.33.10:5000"
)

var (
	concurrent int
	count      int
	c          chan bool
)

var (
	first    int64
	last     int64
	local_ip string
	host     string
	port     int

	redis_pool *redis.Pool
)
