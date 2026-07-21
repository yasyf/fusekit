//go:build race

package catalog

func testScaleCount(full int) int {
	return max(100, full/20)
}
