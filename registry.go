package durable

import (
	"fmt"
	"strconv"
	"strings"
)

// VersionDecision 为启动/恢复时对定义版本的判定结论。
type VersionDecision string

const (
	// VerAcceptCompatible 定义存在且精确版本匹配，按启动时语义执行。
	VerAcceptCompatible VersionDecision = "accept_compatible"
	// VerDefUnknown 定义名完全未知。
	VerDefUnknown VersionDecision = "def_unknown"
	// VerVersionIncompatible 主版本不同：语义不兼容，拒绝。
	VerVersionIncompatible VersionDecision = "version_incompatible"
	// VerVersionMissing 主版本相同但精确版本缺失（仅补丁/小版本被移除）。
	VerVersionMissing VersionDecision = "version_missing"
)

// Registry 保存当前进程已知的编排定义，负责版本兼容判定。
type Registry struct {
	// defs[name][version] = Definition（Definition 注册后视为不可变）。
	defs map[string]map[string]Definition
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{defs: map[string]map[string]Definition{}}
}

// Register 注册一个不可变定义；同名同版本重复注册必须完全一致，否则拒绝。
func (r *Registry) Register(d Definition) error {
	if d.Name == "" {
		return &RejectError{Code: ReasonDefUnknown, Message: "definition name is empty"}
	}
	if err := validateSemver(d.Version); err != nil {
		return &RejectError{Code: ReasonDefVersionIncompat, Message: err.Error()}
	}
	for _, s := range d.Steps {
		if s.Name == "" || s.Fn == nil {
			return &RejectError{Code: ReasonDefUnknown, Message: fmt.Sprintf("definition %s has invalid step", d.Name)}
		}
	}
	versions, ok := r.defs[d.Name]
	if !ok {
		versions = map[string]Definition{}
		r.defs[d.Name] = versions
	}
	if existing, dup := versions[d.Version]; dup {
		if !sameSteps(existing.Steps, d.Steps) {
			return &RejectError{
				Code:    ReasonDefVersionIncompat,
				Message: fmt.Sprintf("definition %s@%s re-registered with different steps", d.Name, d.Version),
			}
		}
		return nil
	}
	versions[d.Version] = d
	return nil
}

func sameSteps(a, b []Step) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name {
			return false
		}
	}
	return true
}

// Resolve 在注册的定义集合中按名称查找，取版本判定结论。
// 启动路径使用：要求精确版本存在，否则按主版本兼容性给出可区分结论。
func (r *Registry) Resolve(name string) (Definition, VersionDecision) {
	versions, ok := r.defs[name]
	if !ok || len(versions) == 0 {
		return Definition{}, VerDefUnknown
	}
	// 无精确版本概念时，Resolve 仅判断“名称是否已知”。
	// 精确版本的兼容判定统一走 CheckVersion。
	var pick Definition
	for _, d := range versions {
		pick = d
		break
	}
	return pick, VerAcceptCompatible
}

// CheckVersion 判断一个执行启动时钉住的 defVersion 在当前注册集合下是否仍可继续。
// 恢复路径使用：进行中的执行必须按启动时语义继续，不允许静默按新语义重放。
//
// 规则：
//  1. 名称未知 -> def_unknown；
//  2. 精确版本存在 -> accept_compatible（按钉住版本的步骤语义继续）；
//  3. 精确版本不存在但主版本相同 -> version_missing（拒绝，不猜测新语义）；
//  4. 主版本不同 -> version_incompatible（明确拒绝）。
func (r *Registry) CheckVersion(name, defVersion string) (Definition, VersionDecision) {
	versions, ok := r.defs[name]
	if !ok || len(versions) == 0 {
		return Definition{}, VerDefUnknown
	}
	if d, exact := versions[defVersion]; exact {
		return d, VerAcceptCompatible
	}
	pinnedMajor, perr := semverMajor(defVersion)
	if perr != nil {
		return Definition{}, VerVersionIncompatible
	}
	sameMajor := false
	var anyDef Definition
	for v, d := range versions {
		anyDef = d
		m, err := semverMajor(v)
		if err != nil {
			continue
		}
		if m == pinnedMajor {
			sameMajor = true
			break
		}
	}
	if sameMajor {
		return anyDef, VerVersionMissing
	}
	return anyDef, VerVersionIncompatible
}

func validateSemver(v string) error {
	parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
	if len(parts) != 3 {
		return fmt.Errorf("invalid semver %q: want MAJOR.MINOR.PATCH", v)
	}
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n < 0 {
			return fmt.Errorf("invalid semver %q: %q is not a non-negative integer", v, p)
		}
	}
	return nil
}

func semverMajor(v string) (int, error) {
	if err := validateSemver(v); err != nil {
		return 0, err
	}
	m, _ := strconv.Atoi(strings.Split(strings.TrimPrefix(v, "v"), ".")[0])
	return m, nil
}

// Unregister 移除一个已注册定义，用于模拟运行期定义变更。
func (r *Registry) Unregister(name, version string) {
	if vs, ok := r.defs[name]; ok {
		delete(vs, version)
		if len(vs) == 0 {
			delete(r.defs, name)
		}
	}
}
