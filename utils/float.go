package utils

import "strconv"

// FloatFromBytes 从字节数组中转成float64
func FloatFromBytes(v []byte) float64 {
	float, _ := strconv.ParseFloat(string(v), 64)
	return float
}

// Float64ToBytes 转成字符串再变成字节
func Float64ToBytes(v float64) []byte {
	return []byte(strconv.FormatFloat(v, 'f', -1, 64))
}
