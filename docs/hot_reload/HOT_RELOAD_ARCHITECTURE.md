# Hot Reloading Architecture for Lunatic

> **Historical design document.** This file records the original proposal and
> is not a current CLI or API reference. The supported file-triggered command is
> `lunatic run --watch <module.wasm>`. For implemented behavior, executable
> evidence, and remaining limits, see
> [Core Values Status](../core_values/status.md) and
> [Watch-Mode Hot Reload](HOT_RELOAD_MVP.md).

## Executive Summary

This document proposes an architecture for adding Hot Code Reloading functionality to the Lunatic runtime. It is inspired by Erlang/BEAM's hot code loading mechanism and designed considering WebAssembly's characteristics and Lunatic's current architecture.

## 1. 현재 아키텍처 분석

### 1.1 주요 컴포넌트

**Process (`lunatic-process/src/lib.rs`)**
- `WasmProcess`: Running WASM module instance
- Signal-based inter-process communication
- Identified by Process ID and Signal mailbox

**Environment (`lunatic-process/src/env.rs`)**
- `LunaticEnvironment`: Environment managing processes
- Stores processes in `DashMap<u64, Arc<dyn Process>>`
- Provides process creation/removal/lookup functionality

**Module Compilation (`lunatic-process/src/runtimes/wasmtime.rs`)**
- `WasmtimeRuntime`: WASM module compilation engine
- `WasmtimeCompiledModule`: Compiled module (shared via Arc)
- `compile_module()`: Compiles modules
- `instantiate()`: Creates instances

**Module Resources (`lunatic-process-api/src/lib.rs`)**
- `ModuleResources<S>`: `HashMapId<Arc<WasmtimeCompiledModule<S>>>`
- Manages module resources per process
- `compile_module()`, `drop_module()` host functions

**State Management (`src/state.rs`)**
- `DefaultProcessState`: 프로세스 상태 저장
- `module: Option<Arc<WasmtimeCompiledModule<Self>>>`
- `runtime: Option<WasmtimeRuntime>`
- `signal_mailbox`, `message_mailbox` 등

### 1.2 현재 제약사항

1. **파일 감시 없음**: `notify`, `inotify` 등의 파일 감시 라이브러리 미사용
2. **모듈 버전 관리 없음**: 하나의 모듈만 메모리에 로드
3. **상태 전환 메커니즘 없음**: 프로세스 상태를 새 코드로 마이그레이션하는 기능 없음
4. **Hot reload 명시적 표시**: `README.md`에 "Hot reloading" 항목이 체크되지 않음

## 2. Erlang/BEAM Hot Code Loading 메커니즘

### 2.1 핵심 개념

**Two-Version Policy**
- VM은 하나의 모듈에 대해 두 버전을 동시에 메모리에 유지
- 세 번째 버전이 로드되면 가장 오래된 버전을 실행하는 프로세스들이 종료됨

**Local vs External Calls**
- Local call (`func()`): 현재 버전에서 계속 실행
- External call (`module:func()`): 항상 최신 버전으로 실행
- Tail-recursive loop에서 `?MODULE:loop(State)`를 사용하여 버전 전환

**Code Server**
- ETS 테이블로 모듈 버전 관리
- `code:load_file/1`, `code:purge/1` 등의 API 제공

**State Transformation**
- `Module:code_change(OldVsn, State, Extra)` 콜백
- 이전 버전의 상태를 새 버전의 상태로 변환

### 2.2 Hot Reload 프로세스

```
1. 새 버전 컴파일
2. Code Server에 로드
3. External call 통해 프로세스가 새 버전으로 전환
4. code_change 콜백으로 상태 변환
5. 오래된 버전 purge (선택적)
```

## 3. WebAssembly 특성 고려사항

### 3.1 WASM의 제약

- **No runtime code modification**: 실행 중인 인스턴스의 코드 수정 불가
- **Stateless by default**: 모든 상태는 linear memory나 host에 저장
- **Module-level granularity**: 함수 단위가 아닌 모듈 단위로 교체

### 3.2 WASM의 장점

- **Clean boundaries**: 모듈 간 명확한 경계
- **Serializable state**: Linear memory를 직접 복사 가능
- **Fast instantiation**: Wasmtime의 `InstancePre`로 빠른 인스턴스 생성

## 4. 제안 아키텍처

### 4.1 컴포넌트 구조

```
┌─────────────────────────────────────────────┐
│          File Watcher (notify crate)        │
│  - Watch .wasm files                        │
│  - Debounce changes                         │
└─────────────────┬───────────────────────────┘
                  │ File changed event
                  ▼
┌─────────────────────────────────────────────┐
│         Module Version Manager              │
│  - Track module versions (v1, v2, ...)     │
│  - Compile new versions                     │
│  - Maintain module metadata                 │
└─────────────────┬───────────────────────────┘
                  │ New module ready
                  ▼
┌─────────────────────────────────────────────┐
│        Process Reload Coordinator           │
│  - Send reload signals to processes         │
│  - Coordinate state transfer                │
│  - Handle rollback on failure               │
└─────────────────┬───────────────────────────┘
                  │ Reload signal
                  ▼
┌─────────────────────────────────────────────┐
│           WasmProcess (Enhanced)            │
│  - Receive reload signal                    │
│  - Serialize current state                  │
│  - Create new instance with new module      │
│  - Deserialize state into new instance      │
│  - Switch execution to new instance         │
└─────────────────────────────────────────────┘
```

### 4.2 데이터 구조

#### ModuleVersion
```rust
pub struct ModuleVersion {
    pub id: u64,
    pub version: u32,
    pub module: Arc<WasmtimeCompiledModule<T>>,
    pub source_path: Option<PathBuf>,
    pub loaded_at: SystemTime,
    pub process_count: AtomicUsize,  // 이 버전을 사용하는 프로세스 수
}
```

#### ModuleRegistry
```rust
pub struct ModuleRegistry<T> {
    modules: DashMap<u64, Vec<ModuleVersion>>,  // module_id -> versions
    max_versions: usize,  // 기본값: 2 (Erlang처럼)
}

impl<T> ModuleRegistry<T> {
    pub fn add_version(&self, id: u64, module: WasmtimeCompiledModule<T>) -> u32;
    pub fn get_latest(&self, id: u64) -> Option<Arc<WasmtimeCompiledModule<T>>>;
    pub fn get_version(&self, id: u64, version: u32) -> Option<Arc<WasmtimeCompiledModule<T>>>;
    pub fn purge_old_versions(&self, id: u64);
}
```

#### ReloadableState
```rust
pub trait ReloadableState {
    /// 현재 상태를 직렬화
    fn serialize_state(&self) -> Result<Vec<u8>>;
    
    /// 새 인스턴스에 상태 복원
    fn deserialize_state(&mut self, data: &[u8]) -> Result<()>;
    
    /// 버전 간 상태 변환 (선택적)
    fn transform_state(&self, from_version: u32, to_version: u32, data: Vec<u8>) -> Result<Vec<u8>> {
        Ok(data)  // 기본 구현: 그대로 전달
    }
}
```

### 4.3 New Signal Types

```rust
pub enum Signal {
    // 기존 signals...
    Message(Message),
    Kill,
    Link(Option<i64>, Arc<dyn Process>),
    // ...
    
    // 새로운 signals
    /// Hot reload request
    /// (module_id, new_version)
    HotReload(u64, u32),
    
    /// Request to serialize and report state
    /// (reply_to, reference)
    RequestState(Arc<dyn Process>, u64),
    
    /// Reload completion notification
    /// (success, old_version, new_version)
    ReloadComplete(bool, u32, u32),
}
```

### 4.4 Host Functions

새로운 host function들을 추가:

```rust
// lunatic::process namespace
fn get_module_version(module_id: u64) -> u32;
fn reload_module(module_id: u64) -> Result<u32>;
fn register_hot_reload_handler(handler_fn: &str) -> Result<()>;

// lunatic::module namespace  
fn module_id_from_path(path_ptr: u32, path_len: u32) -> u64;
fn watch_module_file(module_id: u64, path_ptr: u32, path_len: u32) -> Result<()>;
```

## 5. 구현 단계

### Phase 1: Infrastructure (1-2주)
- [ ] `notify` crate 의존성 추가
- [ ] `ModuleRegistry` 구현
- [ ] `ModuleVersion` 추가
- [ ] 파일 감시 기본 구조
- [ ] Signal 타입 확장

### Phase 2: Basic Hot Reload (2-3주)
- [ ] `HotReload` signal 처리
- [ ] 프로세스 재시작 메커니즘
- [ ] 간단한 상태 전송 (메모리 덤프)
- [ ] Host functions 구현
- [ ] 기본 테스트

### Phase 3: State Management (2-3주)
- [ ] `ReloadableState` trait 구현
- [ ] State serialization/deserialization
- [ ] State transformation 콜백 지원
- [ ] Mailbox preservation
- [ ] Link 및 Monitor 유지

### Phase 4: Coordination & Safety (2주)
- [ ] Reload coordinator 구현
- [ ] Rollback 메커니즘
- [ ] 버전 cleanup
- [ ] 에러 처리 강화
- [ ] 메트릭 추가

### Phase 5: File Watching Integration (1-2주)
- [ ] 파일 감시 자동 트리거
- [ ] Debouncing
- [ ] Multi-file 프로젝트 지원
- [ ] Config 파일 지원

### Phase 6: Testing & Documentation (1-2주)
- [ ] Integration tests
- [ ] Example 애플리케이션
- [ ] 문서 작성
- [ ] 벤치마크

**총 예상 기간**: 9-15주

## 6. 핵심 알고리즘

### 6.1 Hot Reload 프로세스

```rust
async fn hot_reload_process(
    process_id: u64,
    old_module_id: u64,
    new_version: u32,
) -> Result<()> {
    // 1. 현재 프로세스 상태 추출
    let state_data = request_state_from_process(process_id).await?;
    
    // 2. 새 모듈 버전 가져오기
    let new_module = module_registry
        .get_version(old_module_id, new_version)
        .ok_or(anyhow!("Module version not found"))?;
    
    // 3. 새 인스턴스 생성
    let new_instance = runtime
        .instantiate(&new_module, state_data)
        .await?;
    
    // 4. 프로세스의 실행 컨텍스트 교체
    swap_process_context(process_id, new_instance)?;
    
    // 5. 새 버전에서 실행 재개
    Ok(())
}
```

### 6.2 State Extraction

프로세스의 실행을 중단하고 상태를 추출하는 전략:

**Option A: Cooperative (권장)**
```rust
// WASM 모듈이 주기적으로 체크포인트 제공
#[no_mangle]
pub extern "C" fn lunatic_checkpoint() {
    // Host가 HotReload signal 확인
    // 있으면 상태 직렬화하고 yield
}
```

**Option B: Preemptive**
```rust
// fuel 메커니즘 활용하여 강제 중단
// 현재 linear memory 전체 복사
// 제한: call stack 손실 가능
```

### 6.3 Version Transition Guard

```rust
impl<T> ModuleRegistry<T> {
    pub fn add_version(&self, id: u64, module: WasmtimeCompiledModule<T>) -> u32 {
        let mut versions = self.modules.entry(id).or_insert_with(Vec::new);
        let new_version = versions.len() as u32;
        
        versions.push(ModuleVersion {
            id,
            version: new_version,
            module: Arc::new(module),
            loaded_at: SystemTime::now(),
            process_count: AtomicUsize::new(0),
        });
        
        // 너무 많은 버전이 있으면 정리
        if versions.len() > self.max_versions {
            self.cleanup_old_versions(id, &mut versions);
        }
        
        new_version
    }
    
    fn cleanup_old_versions(&self, id: u64, versions: &mut Vec<ModuleVersion>) {
        // 프로세스가 사용하지 않는 가장 오래된 버전 제거
        versions.retain(|v| {
            v.process_count.load(Ordering::Relaxed) > 0
                || v.version >= (versions.len() - self.max_versions) as u32
        });
    }
}
```

## 7. 사용 예시

### 7.1 Rust 코드 (Guest)

```rust
use lunatic::{process, module};

// 상태 정의
#[derive(Serialize, Deserialize)]
struct CounterState {
    count: i32,
    name: String,
}

#[lunatic::main]
fn main() {
    // 모듈 파일 감시 시작
    let module_id = module::current_module_id();
    module::watch_file(module_id, file!()).unwrap();
    
    let mut state = CounterState {
        count: 0,
        name: "Counter".to_string(),
    };
    
    loop {
        match process::receive() {
            Message::Increment => {
                state.count += 1;
                println!("Count: {}", state.count);
            }
            Message::HotReload => {
                // 상태 저장
                let serialized = bincode::serialize(&state).unwrap();
                process::save_state(&serialized);
                
                // 새 버전으로 전환
                process::reload_with_state().unwrap();
                
                // 상태 복원
                let data = process::get_saved_state();
                state = bincode::deserialize(&data).unwrap();
                
                println!("Reloaded to version {}", module::version());
            }
            _ => {}
        }
    }
}
```

### 7.2 CLI 사용

```bash
# 개발 모드로 실행 (자동 hot reload)
lunatic run --watch myapp.wasm
```

이 설계에서 제안했던 별도 수동 reload/status 명령은 구현된 CLI가 아니다.

## 8. 보안 고려사항

1. **Permission Check**: `can_hot_reload` 권한 추가
2. **Signature Verification**: 새 모듈의 서명 검증 (선택적)
3. **Rollback on Failure**: 실패 시 이전 버전으로 자동 복구
4. **State Validation**: 상태 역직렬화 시 검증

## 9. 성능 고려사항

1. **Instantiation Overhead**: `InstancePre` 사용으로 최소화
2. **State Copy Cost**: 큰 상태는 CoW(Copy-on-Write) 고려
3. **File Watching**: Debouncing으로 불필요한 reload 방지
4. **Memory Usage**: 최대 2개 버전만 유지

## 10. 대안 및 트레이드오프

### 10.1 Strategy A: Full Process Restart (단순)
- 장점: 구현 간단, 상태 관리 불필요
- 단점: 상태 손실, 연결 끊김

### 10.2 Strategy B: In-place Module Swap (제안)
- 장점: 상태 보존, 연결 유지
- 단점: 구현 복잡, 상태 직렬화 필요

### 10.3 Strategy C: Blue-Green Deployment
- 장점: 중단 시간을 줄이는 배포와 롤백을 설계하기 쉬움(현재 보장 아님)
- 단점: 메모리 2배 사용, 상태 동기화 필요

**권장**: Strategy B (In-place Module Swap)

## 11. 참고 자료

1. Erlang Hot Code Loading: http://erlang.org/doc/reference_manual/code_loading.html
2. Wasmtime Module Caching: https://docs.wasmtime.dev/api/wasmtime/struct.Module.html
3. notify crate: https://docs.rs/notify/
4. OTP sys module: https://www.erlang.org/doc/man/sys.html

## 12. 결론

Lunatic에 Hot Reloading을 추가하는 것은 충분히 가능하며, Erlang의 검증된 메커니즘과 WebAssembly의 격리 특성을 결합하여 안전하고 효율적인 구현이 가능합니다.

핵심은:
1. **Two-version module registry**
2. **State serialization/deserialization**
3. **Cooperative reload points**
4. **File watching integration**

이 기능은 개발 생산성을 크게 향상시킬 것이며, Lunatic를 더욱 경쟁력 있는 런타임으로 만들 것입니다.
