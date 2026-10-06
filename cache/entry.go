package cache

import "time"

// entry 是缓存中一个标识对应的不可变快照容器。
// 读取与替换都以整个 entry 为单位，保证任一时刻读到的都是自洽快照，
// 不会出现字段级混合。
type entry struct {
	value     any
	tags      []string
	dependsOn []string
	expiresAt time.Time
	updatedAt time.Time
}

// expired 判断在 now 时刻是否已过期。
func (e *entry) expired(now time.Time) bool {
	return !now.Before(e.expiresAt)
}
