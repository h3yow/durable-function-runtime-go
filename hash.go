package durable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// requestFingerprint 对“同幂等键的请求内容”计算指纹。
// 幂等键相同但 def/version/输入任一不同即视为冲突请求，必须明确拒绝，
// 而不是静默复用既有执行结果。
func requestFingerprint(defName, defVersion string, input json.RawMessage) string {
	var normalized any
	if len(input) > 0 {
		if err := json.Unmarshal(input, &normalized); err != nil {
			// 非 JSON 输入退化为字节序列，仍然可稳定区分。
			normalized = string(input)
		}
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		canonical = []byte(fmt.Sprintf("%v", normalized))
	}
	sum := sha256.Sum256([]byte(defName + "\x00" + defVersion + "\x00" + string(canonical)))
	return hex.EncodeToString(sum[:])
}

func sha256String(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
