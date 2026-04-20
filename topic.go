package pubsub

import (
	"strings"
)

// topicMatch 判断发布的具体 topic 是否匹配订阅的 pattern
func topicMatch(pattern, topic string) bool {
	patternParts := strings.Split(pattern, "/")
	topicParts := strings.Split(topic, "/")

	pi := 0
	ti := 0

	for pi < len(patternParts) && ti < len(topicParts) {
		switch patternParts[pi] {
		case "+":
			pi++
			ti++
		case "#":
			return true
		default:
			if patternParts[pi] != topicParts[ti] {
				return false
			}
			pi++
			ti++
		}
	}

	if pi < len(patternParts) && patternParts[pi] == "#" {
		return true
	}

	return pi == len(patternParts) && ti == len(topicParts)
}

// hasWildcard 判断 topic 是否包含通配符
func hasWildcard(topic string) bool {
	return strings.Contains(topic, "+") || strings.Contains(topic, "#")
}

// isValidTopic 验证 topic 是否合法
func isValidTopic(topic string) bool {
	if topic == "" {
		return false
	}

	parts := strings.Split(topic, "/")
	for i, part := range parts {
		if part == "" {
			return false
		}
		if strings.Contains(part, "#") {
			if len(part) > 1 {
				return false
			}
			if i != len(parts)-1 {
				return false
			}
		}
		if part == "+" {
			continue
		}
		if strings.Contains(part, "+") {
			return false
		}
	}

	return true
}
