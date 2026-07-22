# Hot Reload Demo Instructions

이 데모는 현재 지원되는 `lunatic run --watch` 명령으로 로컬 Wasm 파일
변경을 감지하고, 실행 중인 프로세스에 호환되는 새 모듈을 적용하는 흐름을
보여줍니다.

## 준비

Lunatic과 WABT의 `wat2wasm` 도구가 필요합니다.

```bash
cargo build
wat2wasm examples/simple_loop.wat -o target/hot-reload-demo.wasm
```

## 실행

터미널 1에서 컴파일된 모듈을 감시합니다.

```bash
./target/debug/lunatic run --watch target/hot-reload-demo.wasm
```

`Running v1...` 출력이 반복되는 동안 터미널 2에서 같은 출력 파일을 v2로
다시 컴파일합니다.

```bash
wat2wasm examples/simple_loop_v2.wat -o target/hot-reload-demo.wasm
```

터미널 1에 reload commit 메시지가 나타나고 이후 출력이 `RELOADED v2!`로
바뀌면 파일 감지와 로컬 live-reload 경로가 동작한 것입니다. 이 데모의
화면 출력만으로 메모리·메일박스·리소스 보존 전체를 증명하지는 않습니다.
그 보존 및 실패 시 이전 인스턴스 재개는 자동화 테스트가 검증합니다.

## 동작 범위

- Lunatic은 `.wasm` 변경을 감지하지만 게스트 소스를 자동 컴파일하지
  않습니다.
- 새 모듈은 실행 중인 모듈과 호환되어야 합니다. 검증 또는 인스턴스 준비가
  실패하면 후보 버전은 커밋되지 않고 이전 인스턴스가 계속 실행됩니다.
- 검증된 범위는 단일 런타임의 로컬 프로세스입니다. 분산·노드 간 reload는
  검증되지 않았습니다.
- 일부 in-process 리소스 이동은 테스트되지만 모든 WASI 또는 애플리케이션
  리소스의 이동을 보장하지 않습니다.
- 현재 벤치마크는 end-to-end reload 지연, 무중단 동작, 클러스터 규모를
  입증하지 않습니다.

정확한 현재 상태는 [Core Values Status](../docs/core_values/status.md), 실행
가능한 회귀 테스트는
[`tests/live_hot_reload.rs`](../tests/live_hot_reload.rs)를 참고하세요.
