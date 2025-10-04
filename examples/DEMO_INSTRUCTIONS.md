# Hot Reload Demo Instructions

## 체험할 수 있는 것

현재 구현된 **--watch 모드**로 hot reload의 기본 동작을 체험할 수 있습니다.

## 준비

```bash
# 1. Lunatic 빌드
cargo build

# 2. 데모 앱 컴파일
wat2wasm examples/demo_app.wat -o /tmp/demo_app.wasm
```

## 데모 실행 (Option 1: 간단한 WAT 파일)

### Terminal 1: Watch 모드로 실행
```bash
# Counter 예제 실행
wat2wasm examples/counter_v1.wat -o /tmp/counter.wasm
./target/debug/lunatic run --watch /tmp/counter.wasm
```

### Terminal 2: 파일 변경
```bash
# v2로 업데이트
wat2wasm examples/counter_v2.wat -o /tmp/counter.wasm
```

**결과:** Terminal 1에서 프로세스가 자동으로 재시작됨!

## 데모 실행 (Option 2: 실제 개발 워크플로우)

더 실감나는 체험을 위해 Rust 애플리케이션으로:

### 1. Rust 예제 프로젝트 생성
```bash
cd /tmp
cargo new --bin lunatic-demo
cd lunatic-demo

# Cargo.toml에 추가
cat >> Cargo.toml << 'EOF'

[dependencies]
lunatic = "0.13"

[profile.release]
opt-level = "z"

[[bin]]
name = "demo"
path = "src/main.rs"
EOF
```

### 2. 간단한 애플리케이션 작성
```rust
// src/main.rs
use lunatic::{process, Mailbox};
use std::thread;
use std::time::Duration;

#[lunatic::main]
fn main(_: Mailbox<()>) {
    let mut counter = 0;
    loop {
        counter += 1;
        println!("Version 1: Counter = {}", counter);
        thread::sleep(Duration::from_secs(1));
    }
}
```

### 3. 빌드 및 실행
```bash
# Terminal 1: Watch 모드 실행
rustup target add wasm32-wasi
cargo build --target wasm32-wasi
/path/to/lunatic/target/debug/lunatic run --watch target/wasm32-wasi/debug/demo.wasm
```

### 4. 코드 수정 및 핫 리로드
```bash
# Terminal 2: 코드 수정
# src/main.rs를 수정:
println!("Version 2: Counter = {}", counter);  // 메시지 변경
counter += 2;  // 증가량 변경

# 재빌드
cargo build --target wasm32-wasi
```

**결과:** Terminal 1에서:
```
Version 1: Counter = 5
Version 1: Counter = 6
File change detected: /tmp/lunatic-demo/target/wasm32-wasi/debug/demo.wasm
Restarting process...
Version 2: Counter = 1  # ← 새 버전으로 재시작!
Version 2: Counter = 3
Version 2: Counter = 5
```

## 현재 동작 방식

**Phase 3 (현재):**
```
코드 수정 → 컴파일 → .wasm 변경 → 파일 감지 → 프로세스 재시작
                                              ↑
                                    여기까지 구현됨!
```

**특징:**
- ✅ 자동 감지: 파일 변경 즉시 반응
- ✅ 빠른 재시작: 수동 재시작보다 빠름
- ⚠️ 상태 손실: Counter가 1부터 다시 시작
- ⚠️ 프로세스 종료/재시작: 완전히 새로운 프로세스

**미래 (Phase 4-6 구현 후):**
```
코드 수정 → 컴파일 → .wasm 변경 → 파일 감지 → HOT RELOAD
                                              ↓
                                    상태 보존된 채 코드만 교체
                                    Counter 값 유지!
```

## 체험 시나리오

### 시나리오 1: 메시지 변경
```rust
// Before
println!("Hello {}", counter);

// After  
println!("Greetings {}", counter);
```
→ 메시지가 바로 바뀜

### 시나리오 2: 로직 변경
```rust
// Before
counter += 1;

// After
counter += 2;
```
→ 증가 속도가 바뀜 (하지만 counter는 리셋됨)

### 시나리오 3: 새 기능 추가
```rust
// After
if counter % 10 == 0 {
    println!("Milestone: {}", counter);
}
```
→ 새 기능이 즉시 반영됨

## 개선 사항 확인

**수동 재시작 (이전):**
```bash
# 매번 해야 하는 작업:
1. Ctrl+C로 종료
2. cargo build --target wasm32-wasi
3. lunatic run ...
4. 다시 시작
```

**--watch 모드 (현재):**
```bash
# 한 번만:
1. lunatic run --watch ...

# 이후 변경 시:
1. cargo build --target wasm32-wasi
2. 자동으로 재시작됨!
```

## 제한사항 (현재)

현재 체험할 수 있는 것:
- ✅ 자동 재시작
- ✅ 빠른 개발 사이클
- ✅ 파일 변경 감지

아직 체험할 수 없는 것:
- ❌ 상태 보존 (counter 값 유지)
- ❌ WebSocket 연결 유지
- ❌ 진행 중인 작업 계속

→ 이것들은 Phase 4-6에서 구현 예정!

## 결론

**현재 Phase 3로도 충분히 유용합니다:**
- 개발 중 수동 재시작 불필요
- 코드 변경 후 즉시 확인
- 개발 생산성 향상

**완전한 hot reload는 아니지만**, 개발 경험은 이미 크게 개선되었습니다!
