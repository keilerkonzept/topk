package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/keilerkonzept/topk"
)

func main() {
	fileName := flag.String("f", "", "file name")
	k := flag.Uint("k", 10, "find k top values")
	width := flag.Uint("w", 2048, "array's width, higher value - more memory used but more accurate results")
	depth := flag.Uint("d", 5, "depth, defined amount of buckets in one array")
	decay := flag.Float64("p", 0.9, "probability decay")

	flag.Parse()

	var reader io.Reader
	if *fileName == "" {
		reader = os.Stdin
	} else {
		var err error
		reader, err = os.Open(*fileName)
		if err != nil {
			log.Fatal(err)
		}
	}

	sketch := topk.New(int(*k), topk.WithWidth(int(*width)), topk.WithDepth(int(*depth)), topk.WithDecay(float32(*decay)))

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {

		for _, item := range strings.Split(scanner.Text(), " ") {
			if item != "" {
				sketch.Add(item, 1)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	for _, entry := range sketch.SortedSlice() {
		fmt.Printf("%s : %d\n", entry.Item, entry.Count)
	}

}
