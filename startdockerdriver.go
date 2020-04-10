package main

import (
	"log"

	"github.com/tsuru/bs/log/dockerdriver"
)

func main() {
	d := dockerdriver.New()
	if err := d.Start(); err != nil {
		log.Fatal(err)
	}
}
