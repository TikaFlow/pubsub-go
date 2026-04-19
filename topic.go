package pubsub

import (
    "errors"
    "strings"
)

var (
    ErrEmptyTopic       = errors.New("pubsub: topic must not be empty")
    ErrInvalidWildcard  = errors.New("pubsub: '#' must be the last character in topic")
    ErrInvalidTopicLevel = errors.New("pubsub: topic level must not be empty")
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

// isConcreteTopic 判断 topic 是否为具体主题（不含通配符）
func isConcreteTopic(topic string) bool {
    return !strings.Contains(topic, "+") && !strings.Contains(topic, "#")
}

// hasWildcard 判断 topic 是否包含通配符
func hasWildcard(topic string) bool {
    return strings.Contains(topic, "+") || strings.Contains(topic, "#")
}

// validateTopic 验证 topic 是否合法
func validateTopic(topic string) error {
    if topic == "" {
        return ErrEmptyTopic
    }

    parts := strings.Split(topic, "/")
    for i, part := range parts {
        if part == "" {
            return ErrInvalidTopicLevel
        }
        if strings.Contains(part, "#") {
            if len(part) > 1 {
                return ErrInvalidWildcard
            }
            if i != len(parts)-1 {
                return ErrInvalidWildcard
            }
        }
        if part == "+" {
            continue
        }
        if strings.Contains(part, "+") {
            return ErrInvalidTopicLevel
        }
    }

    return nil
}
