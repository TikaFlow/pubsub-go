package pubsub

import (
	"strings"
)

// TopicMatch 判断发布的具体 topic 是否匹配订阅的 pattern
func TopicMatch(pattern, topic string) bool {
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

// HasWildcard 判断 topic 是否包含通配符
func HasWildcard(topic string) bool {
	return strings.Contains(topic, "+") || strings.Contains(topic, "#")
}

// IsValidTopic 验证 topic 是否合法
func IsValidTopic(topic string) bool {
	if topic == "" {
		return false
	}

	parts := strings.Split(topic, "/")
	last := len(parts) - 1
	for i, part := range parts {
		if part == "" {
			return false
		}

		if part == "+" {
			continue
		}

		if part == "#" && i == last {
			return true
		}

		if HasWildcard(part) {
			return false
		}
	}

	return true
}
