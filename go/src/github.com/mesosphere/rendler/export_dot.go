package rendler

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

const (
	output = "result.dot"
)

func WriteDOTFile(crawlResults []*Edge, renderResults map[string]string) error {
	fo, err := os.Create(output)
	if err != nil {
		return err
	}
	defer fo.Close()
	w := bufio.NewWriter(fo)

	_, err = w.WriteString("digraph {\n")
	if err != nil {
		return err
	}
	_, err = w.WriteString("  node [shape=box];\n")
	if err != nil {
		return err
	}

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

		_, err = w.WriteString("  X" + hash + " [label=\"\" image=\"" + path + "\"];\n")
		if err != nil {
			return err
		}
	}

	for _, e := range crawlResults {
		from := e.From
		from_hash_bytes := sha256.Sum256([]byte(from))
		from_hash := hex.EncodeToString(from_hash_bytes[:32])

		to := e.To
		to_hash_bytes := sha256.Sum256([]byte(to))
		to_hash := hex.EncodeToString(to_hash_bytes[:32])

		if _, ok := urlsWithImages[to_hash]; !ok {
			continue
		}
		if _, ok := urlsWithImages[from_hash]; !ok {
			continue
		}
		_, err = w.WriteString("  X" + from_hash + " -> X" + to_hash + ";\n")
		if err != nil {
			return err
		}
	}

	_, err = w.WriteString("}\n")
	if err != nil {
		return err
	}
	err = w.Flush()
	if err != nil {
		return err
	}

	fmt.Printf("Results written to '%s'\n", output)
	return nil
}
