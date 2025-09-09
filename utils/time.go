package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

func ParseISO8601Duration(iso string) (time.Duration, error) {

	re := regexp.MustCompile(`(\d+)([HMS])`)

	matches := re.FindAllStringSubmatch(iso, -1)

	if matches == nil {
		return 0, fmt.Errorf("không thể phân tích chuỗi duration ISO 8601: %s", iso)
	}

	var totalDuration time.Duration

	for _, match := range matches {
		valueStr := match[1]
		unit := match[2]

		value, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("không thể chuyển đổi giá trị '%s' thành số: %w", valueStr, err)
		}

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
