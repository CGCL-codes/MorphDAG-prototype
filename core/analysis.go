package core

import (
	"github.com/PlainDAG/go-PlainDAG/core/ttype"
	"math"
)

// CalculateConcurrency calculates resilient block concurrency according to the pending pool size
func CalculateConcurrency(scale, preCon int) int {
	var resCon int
	// coefficient after fitting
	//resCon = int(math.Ceil(0.008849*float64(scale) - 2.048))
	//resCon = int(math.Ceil(0.005093*float64(scale) - 2.419))
	resCon = int(math.Ceil(0.005565*float64(scale) - 1.894))
	if resCon > SafeConcurrency {
		// current block concurrency cannot exceed the safety threshold
		return SafeConcurrency
	}
	return resCon
}

// AnalyzeHotAccounts analyzes hot accounts in all concurrent blocks in the current epoch
func AnalyzeHotAccounts(txs map[string][]*ttype.Transaction, frequency int) map[string]struct{} {
	var sum = make(map[string]int)
	var hotAccounts = make(map[string]struct{})

	for _, set := range txs {
		for _, tx := range set {
			for acc := range tx.Payload {
				sum[acc]++
			}
		}
	}

	for acc, count := range sum {
		if count >= frequency {
			hotAccounts[acc] = struct{}{}
		}
	}

	return hotAccounts
}
