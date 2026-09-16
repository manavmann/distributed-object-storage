// Command coordinator runs the Cairn control plane: HTTP API, metadata, placement.
package main

import "fmt"

const version = "0.1.0"

func main() {
	fmt.Printf("coordinator %s\n", version)
}
