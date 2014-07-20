package rendler

import (
	"bufio"
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

func WriteDOTFile(crawlResults *list.List, renderResults map[string]string) {
	fo, err := os.Create("result.dot")
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(fo)

	w.WriteString("digraph {\n")
	w.WriteString("  node [shape=box];\n")

	urlsWithImages := make(map[string]string)

	for k, v := range renderResults {
		fmt.Printf("render:%s:%s\n", k, v)

		hash_bytes := sha256.Sum256([]byte(k))
		hash := hex.EncodeToString(hash_bytes[:32])

		urlsWithImages[hash] = k

		// TODO(nnielsen): Support remote mode.
		localFile := "file:///"
		path := ""
		if strings.HasPrefix(v, localFile) {
			path = v[len(localFile):]
		}

		w.WriteString("  X" + hash + " [label=\"\" image=\"" + path + "\"];\n")
	}

	for e := crawlResults.Front(); e != nil; e = e.Next() {
		from := e.Value.(Edge).From
		from_hash_bytes := sha256.Sum256([]byte(from))
		from_hash := hex.EncodeToString(from_hash_bytes[:32])

		to := e.Value.(Edge).To
		to_hash_bytes := sha256.Sum256([]byte(to))
		to_hash := hex.EncodeToString(to_hash_bytes[:32])

		if _, ok := urlsWithImages[to_hash]; !ok {
			continue
		}
		if _, ok := urlsWithImages[from_hash]; !ok {
			continue
		}
		w.WriteString("  X" + from_hash + " -> X" + to_hash + ";\n")
	}

	w.WriteString("}\n")

	w.Flush()
	fo.Close()

	fmt.Println("Results written to 'result.dot'")
}
