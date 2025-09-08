package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// ParseISO8601Duration parses a duration string in ISO 8601 format (e.g., "PT20M", "PT1H30M").
// It handles common time units: H (hours), M (minutes), S (seconds).
func ParseISO8601Duration(iso string) (time.Duration, error) {
	// Biểu thức chính quy để tìm các cặp giá trị-đơn vị (ví dụ: "20M", "1H")
	// Nó tìm kiếm một hoặc nhiều chữ số (\d+) theo sau bởi một trong các ký tự H, M, S.
	re := regexp.MustCompile(`(\d+)([HMS])`)

	// Tìm tất cả các cặp phù hợp trong chuỗi
	matches := re.FindAllStringSubmatch(iso, -1)

	if matches == nil {
		return 0, fmt.Errorf("không thể phân tích chuỗi duration ISO 8601: %s", iso)
	}

	var totalDuration time.Duration

	// Lặp qua tất cả các cặp đã tìm thấy
	for _, match := range matches {
		valueStr := match[1]
		unit := match[2]

		// Chuyển đổi giá trị thành số nguyên 64-bit
		value, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("không thể chuyển đổi giá trị '%s' thành số: %w", valueStr, err)
		}

		// Cộng dồn duration dựa trên đơn vị
		switch unit {
		case "H":
			totalDuration += time.Duration(value) * time.Hour
		case "M":
			totalDuration += time.Duration(value) * time.Minute
		case "S":
			totalDuration += time.Duration(value) * time.Second
		}
	}

	return totalDuration, nil
}
