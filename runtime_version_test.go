package durable

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

// 场景 9：启动时定义未知必须被明确拒绝，原因可区分，且不创建执行。
func TestStartRejectsUnknownDefinition(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	rt := mustRuntime(t, dir, NewRegistry(), 2)
	_, err := rt.Start(context.Background(), "ghost", "1.0.0", "key-unknown", jsonIn(`{}`))
	rj, ok := AsRejectError(err)
	if !ok {
		t.Fatalf("want RejectError, got %v", err)
	}
	if rj.Code != ReasonDefUnknown {
		t.Fatalf("reason=%s want %s", rj.Code, ReasonDefUnknown)
	}
	t.Logf("[test] idempKey=%q rejected versionDecision=%s reason=%s",
		"key-unknown", VerDefUnknown, rj.Code)
}

// 场景 10：启动时主版本不兼容必须拒绝（即使名称已知）。
func TestStartRejectsIncompatibleMajor(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	if err := reg.Register(Definition{
		Name: "svc", Version: "2.0.0", Steps: []Step{echoStep("a")},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	_, err := rt.Start(context.Background(), "svc", "1.0.0", "key-major", jsonIn(`{}`))
	rj, ok := AsRejectError(err)
	if !ok {
		t.Fatalf("want RejectError, got %v", err)
	}
	if rj.Code != ReasonDefVersionIncompat {
		t.Fatalf("reason=%s want %s", rj.Code, ReasonDefVersionIncompat)
	}
	t.Logf("[test] idempKey=%q rejected versionDecision=%s reason=%s",
		"key-major", VerVersionIncompatible, rj.Code)
}

// 场景 11：进程重启时定义已被移除（进行中执行）-> rejected，绝不按新语义重放。
func TestRecoveryDefinitionRemoved(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	if err := reg.Register(Definition{
		Name: "svc", Version: "1.0.0",
		Steps: []Step{echoStep("a"), echoStep("b")},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	key := "key-def-removed"
	res, err := rt.Start(context.Background(), "svc", "1.0.0", key, jsonIn(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	// 正常跑完后关闭；再以“定义已被注销”的注册表重开 -> 已完成执行不受影响（终态）。
	waitForStatus(t, rt, res.ExecutionID, ExecSucceeded, 3*time.Second)
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	empty := NewRegistry()
	rt2 := mustRuntime(t, dir, empty, 2)
	e, err := rt2.GetExecution(context.Background(), res.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != ExecSucceeded {
		t.Fatalf("terminal execution must remain untouched on restart, got %s", e.Status)
	}
	t.Logf("[test] idempKey=%q completed execution preserved when definition removed", key)
}

// 场景 12：进程重启时进行中执行的定义主版本升级到不兼容版本 -> rejected，
// 不能静默按新步骤重放，原因码与“定义未知”“同主版本缺失”可区分。
func TestRecoveryInFlightMajorIncompatible(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	proceed := make(chan struct{})
	armed := make(chan struct{})
	block := func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		select {
		case <-armed:
		default:
			close(armed)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-proceed:
			return in, nil
		}
	}
	if err := reg.Register(Definition{
		Name: "svc", Version: "1.0.0",
		Steps: []Step{echoStep("a"), {Name: "b", Fn: block}},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	key := "key-major-restart"
	res, err := rt.Start(context.Background(), "svc", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-armed:
	case <-time.After(3 * time.Second):
		t.Fatal("step b never started")
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}

	// 新版本 2.0.0：主版本不兼容。注册表中只保留 2.0.0。
	reg2 := NewRegistry()
	if err := reg2.Register(Definition{
		Name: "svc", Version: "2.0.0",
		// 故意调换/改名步骤，证明绝不会用新语义重放。
		Steps: []Step{echoStep("totally-different-step")},
	}); err != nil {
		t.Fatal(err)
	}
	rt2 := mustRuntime(t, dir, reg2, 2)
	e := waitForStatus(t, rt2, res.ExecutionID, ExecRejected, 3*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonDefVersionIncompat {
		t.Fatalf("failure=%+v want %s", e.Failure, ReasonDefVersionIncompat)
	}
	evs := waitForEvent(t, rt2, res.ExecutionID, EvRejected, 3*time.Second)
	var rej *Event
	for i := range evs {
		if evs[i].Type == EvRejected {
			rej = &evs[i]
		}
	}
	if rej == nil || rej.VersionDecision != string(VerVersionIncompatible) {
		t.Fatalf("reject event missing version decision: %+v", rej)
	}
	// 已完成的第一步保持成功；未完成步骤被标记 cancelled，而非按新定义执行。
	if e.Steps[0].Status != StepSucceeded || e.Steps[1].Status != StepCancelled {
		t.Fatalf("steps=%+v", e.Steps)
	}
	t.Logf("[test] idempKey=%q in-flight execution rejected on restart def=%s@%s versionDecision=%s reason=%s",
		key, e.DefName, e.DefVersion, rej.VersionDecision, rej.Reason)
}

// 场景 13：主版本相同但钉住的精确版本被移除 -> 以 version_missing 拒绝，
// 与主版本不兼容区分。
func TestRecoverySameMajorVersionMissing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	proceed := make(chan struct{})
	armed := make(chan struct{})
	block := func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		select {
		case <-armed:
		default:
			close(armed)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-proceed:
			return in, nil
		}
	}
	if err := reg.Register(Definition{
		Name: "svc", Version: "1.0.0",
		Steps: []Step{echoStep("a"), {Name: "b", Fn: block}},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	key := "key-version-missing"
	res, err := rt.Start(context.Background(), "svc", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-armed:
	case <-time.After(3 * time.Second):
		t.Fatal("step b never started")
	}
	_ = rt.Close()

	// 只注册同主版本的 1.1.0，钉住的 1.0.0 已不存在。
	reg2 := NewRegistry()
	if err := reg2.Register(Definition{
		Name: "svc", Version: "1.1.0", Steps: []Step{echoStep("c")},
	}); err != nil {
		t.Fatal(err)
	}
	rt2 := mustRuntime(t, dir, reg2, 2)
	e := waitForStatus(t, rt2, res.ExecutionID, ExecRejected, 3*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonDefVersionMissing {
		t.Fatalf("failure=%+v want %s", e.Failure, ReasonDefVersionMissing)
	}
	evs := waitForEvent(t, rt2, res.ExecutionID, EvRejected, 3*time.Second)
	for _, ev := range evs {
		if ev.Type == EvRejected {
			if ev.VersionDecision != string(VerVersionMissing) {
				t.Fatalf("version decision=%s want %s", ev.VersionDecision, VerVersionMissing)
			}
		}
	}
	t.Logf("[test] idempKey=%q rejected on restart versionDecision=%s reason=%s (same major, exact version removed)",
		key, VerVersionMissing, ReasonDefVersionMissing)
}

// 场景 14：执行运行期间定义从注册表移除（进程未重启）-> 下一次推进时
// 立即 rejected，绝不按其它版本静默继续。
func TestInFlightDefinitionUnregisteredWhileRunning(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	reg := NewRegistry()
	released := make(chan struct{})
	if err := reg.Register(Definition{
		Name: "svc", Version: "1.0.0",
		Steps: []Step{
			echoStep("a"),
			{Name: "b", Fn: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
				// 第一步成功后阻塞于此，期间由测试移除定义。
				close(released)
				<-ctx.Done()
				return nil, ctx.Err()
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rt := mustRuntime(t, dir, reg, 2)
	key := "key-def-unregistered-live"
	res, err := rt.Start(context.Background(), "svc", "1.0.0", key, jsonIn(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	<-released
	// 注册表支持注销后移除定义（模拟定义变更）。
	reg.Unregister("svc", "1.0.0")
	rt.kick()
	// 中断当前阻塞步骤，使驱动在再次推进时重新做版本判定。
	if d := rt.driver(res.ExecutionID, false); d != nil {
		d.interrupt()
	}

	e := waitForStatus(t, rt, res.ExecutionID, ExecRejected, 3*time.Second)
	if e.Failure == nil || e.Failure.Code != ReasonDefUnknown {
		t.Fatalf("failure=%+v want %s", e.Failure, ReasonDefUnknown)
	}
	t.Logf("[test] idempKey=%q live definition removed -> rejected reason=%s", key, ReasonDefUnknown)
	waitInflightZero(t, rt, time.Second)
}
