package main

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru"
)

func main() {
	l, _ := lru.New(5)
	for i := 0; i < 10; i++ {
		l.Add(i, nil)
	}
	for i := 0; i < 10; i++ {
		if _, v := l.Get(i); true {
			fmt.Printf("%+v\n", v)
		}
	}
}
