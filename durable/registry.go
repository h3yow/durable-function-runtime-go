package durable

import (
	"fmt"
	"sync"
)

// Registry 保存编排定义的各版本。
// 进行中的执行按启动时锁定的版本继续；注册新版本不影响已注册的旧版本。
type Registry struct {
	mu   sync.RWMutex
	defs map[string]map[string]*Definition // name -> version -> def
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{defs: map[string]map[string]*Definition{}}
}

// Register 注册一个定义版本。
// 同名同版本重复注册时，步骤结构必须一致，否则 panic（防止静默语义漂移）。
func (r *Registry) Register(def *Definition) {
	if def == nil || def.Name == "" || def.Version == "" {
		panic("durable: definition must have name and version")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	versions, ok := r.defs[def.Name]
	if !ok {
		versions = map[string]*Definition{}
		r.defs[def.Name] = versions
	}
	if existing, ok := versions[def.Version]; ok {
		if !sameShape(existing, def) {
			panic(fmt.Sprintf("durable: definition %s@%s re-registered with different shape", def.Name, def.Version))
		}
		return
	}
	cp := *def
	cp.Steps = append([]StepDef(nil), def.Steps...)
	versions[def.Version] = &cp
}

// sameShape 比较两个定义的步骤名称与顺序是否一致（函数不可比较，按结构判定）。
func sameShape(a, b *Definition) bool {
	if len(a.Steps) != len(b.Steps) {
		return false
	}
	for i := range a.Steps {
		if a.Steps[i].Name != b.Steps[i].Name {
			return false
		}
	}
	return true
}

// Lookup 按名称与版本精确查找定义。
// 名称不存在返回 ErrDefinitionUnknown；版本不存在返回 ErrVersionIncompatible。
func (r *Registry) Lookup(name, version string) (*Definition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions, ok := r.defs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDefinitionUnknown, name)
	}
	def, ok := versions[version]
	if !ok {
		return nil, fmt.Errorf("%w: %s@%s", ErrVersionIncompatible, name, version)
	}
	return def, nil
}
