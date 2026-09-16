// Command storagenode serves checksummed blobs from local disk for a Cairn cluster.
package main

import "fmt"

const version = "0.1.0"

func main() {
	fmt.Printf("storagenode %s\n", version)
}
