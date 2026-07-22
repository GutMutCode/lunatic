package main

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

const (
	statusOK      uint32 = 0
	statusError   uint32 = 1
	statusTimeout uint32 = 9027

	readyTag            int64  = 99
	requestTag          int64  = 41
	replyTag            int64  = 42
	completeTag         int64  = 4202
	normalTimeoutMillis uint64 = 5_000

	noTimeout = ^uint64(0)
)

// The declarations below intentionally mirror the canonical signatures in
// ../../wat/all_imports.wat. The E2E run links the generated module against the
// real runtime, so a signature or namespace mismatch fails before guest code
// starts.

//go:wasmimport lunatic::process create_config
func hostCreateConfig() int64

//go:wasmimport lunatic::process drop_config
func hostDropConfig(configID uint64)

//go:wasmimport lunatic::process process_id
func hostProcessID() uint64

//go:wasmimport lunatic::process spawn
func hostSpawn(
	link int64,
	configID int64,
	moduleID int64,
	functionPtr unsafe.Pointer,
	functionLen uint32,
	paramsPtr unsafe.Pointer,
	paramsLen uint32,
	idPtr unsafe.Pointer,
) uint32

//go:wasmimport lunatic::wasi config_add_command_line_argument
func hostConfigAddCommandLineArgument(configID uint64, argumentPtr unsafe.Pointer, argumentLen uint32)

//go:wasmimport lunatic::error string_size
func hostErrorStringSize(errorID uint64) uint32

//go:wasmimport lunatic::error to_string
func hostErrorToString(errorID uint64, outputPtr unsafe.Pointer)

//go:wasmimport lunatic::error drop
func hostErrorDrop(errorID uint64)

//go:wasmimport lunatic::message create_data
func hostMessageCreateData(tag int64, capacity uint64)

//go:wasmimport lunatic::message data_size
func hostMessageDataSize() uint64

//go:wasmimport lunatic::message get_tag
func hostMessageGetTag() int64

//go:wasmimport lunatic::message read_data
func hostMessageReadData(dataPtr unsafe.Pointer, dataLen uint32) uint32

//go:wasmimport lunatic::message receive
func hostMessageReceive(tagsPtr unsafe.Pointer, tagsLen uint32, timeoutMillis uint64) uint32

//go:wasmimport lunatic::message send
func hostMessageSend(processID uint64) uint32

//go:wasmimport lunatic::message send_receive_skip_search
func hostMessageSendReceiveSkipSearch(
	processID uint64,
	waitOnTag int64,
	timeoutMillis uint64,
) uint32

//go:wasmimport lunatic::message write_data
func hostMessageWriteData(dataPtr unsafe.Pointer, dataLen uint32) uint32

func bytesPointer(data []byte) unsafe.Pointer {
	if len(data) == 0 {
		return nil
	}
	return unsafe.Pointer(&data[0])
}

func addCommandLineArgument(configID uint64, argument string) {
	data := []byte(argument)
	hostConfigAddCommandLineArgument(configID, bytesPointer(data), uint32(len(data)))
	runtime.KeepAlive(data)
}

func errorString(errorID uint64) string {
	size := hostErrorStringSize(errorID)
	data := make([]byte, size)
	if len(data) != 0 {
		hostErrorToString(errorID, bytesPointer(data))
		runtime.KeepAlive(data)
	}
	hostErrorDrop(errorID)
	return string(data)
}

func spawnStart(configID int64) (uint32, uint64) {
	entrypoint := []byte("_start")
	var processOrErrorID uint64
	status := hostSpawn(
		0,
		configID,
		-1,
		bytesPointer(entrypoint),
		uint32(len(entrypoint)),
		nil,
		0,
		unsafe.Pointer(&processOrErrorID),
	)
	runtime.KeepAlive(entrypoint)
	runtime.KeepAlive(&processOrErrorID)
	return status, processOrErrorID
}

func createInt64Message(tag int64, value int64) {
	hostMessageCreateData(tag, 8)
	if written := hostMessageWriteData(unsafe.Pointer(&value), 8); written != 8 {
		panic("Lunatic did not accept the complete i64 message")
	}
	runtime.KeepAlive(&value)
}

func readInt64Message() int64 {
	if size := hostMessageDataSize(); size != 8 {
		panic("Lunatic returned an unexpected message size")
	}
	var value int64
	if read := hostMessageReadData(unsafe.Pointer(&value), 8); read != 8 {
		panic("Lunatic did not return the complete i64 message")
	}
	runtime.KeepAlive(&value)
	return value
}

func receiveTag(tag int64, timeoutMillis uint64) uint32 {
	status := hostMessageReceive(unsafe.Pointer(&tag), 1, timeoutMillis)
	runtime.KeepAlive(&tag)
	return status
}

func notifyObserver(observerID uint64, tag int64) {
	hostMessageCreateData(tag, 0)
	if status := hostMessageSend(observerID); status != statusOK {
		panic("failed to notify the observer")
	}
}

func runChild(observerID uint64) {
	// create_config creates children with spawning denied. Calling spawn with
	// the current child config must therefore return a guest-readable error
	// instead of creating a grandchild or trapping.
	status, errorID := spawnStart(-1)
	if status != statusError {
		panic("an attenuated child unexpectedly retained spawn permission")
	}
	denial := errorString(errorID)
	if !strings.Contains(denial, "permissions to spawn") {
		panic("spawn denial did not contain the expected permission error")
	}

	// The observer does not send the request until it receives readyTag, so the
	// empty mailbox makes this a deterministic exercise of status 9027.
	if status := hostMessageReceive(nil, 0, 5); status != statusTimeout {
		panic("empty mailbox receive did not return timeout status 9027")
	}

	notifyObserver(observerID, readyTag)

	if status := hostMessageReceive(nil, 0, noTimeout); status != statusOK {
		panic("child did not receive the observer request")
	}
	if tag := hostMessageGetTag(); tag != requestTag {
		panic("child received an unexpected request tag")
	}
	if value := readInt64Message(); value != requestTag {
		panic("child received an unexpected request value")
	}

	createInt64Message(replyTag, replyTag)
	if status := hostMessageSend(observerID); status != statusOK {
		panic("child could not send the tagged reply")
	}
}

func runRoot(observerID uint64) {
	rootID := hostProcessID()
	configID := hostCreateConfig()
	if configID < 0 {
		panic("root process could not create an attenuated child config")
	}
	childConfigID := uint64(configID)

	// A fresh TinyGo command instance must enter through _start so its runtime
	// is initialized. WASI argv selects the child role without relying on a Go
	// function export that would run before initialization.
	addCommandLineArgument(childConfigID, "child")
	addCommandLineArgument(childConfigID, strconv.FormatUint(rootID, 10))

	status, processOrErrorID := spawnStart(configID)
	hostDropConfig(childConfigID)
	if status != statusOK {
		if status == statusError {
			panic("failed to spawn child: " + errorString(processOrErrorID))
		}
		panic("failed to spawn child with an unexpected status")
	}
	childID := processOrErrorID

	if status := receiveTag(readyTag, normalTimeoutMillis); status != statusOK {
		panic("observer did not receive the child readiness notification")
	}
	if tag := hostMessageGetTag(); tag != readyTag {
		panic("observer received an unexpected readiness tag")
	}

	createInt64Message(requestTag, requestTag)
	if status := hostMessageSendReceiveSkipSearch(childID, replyTag, normalTimeoutMillis); status != statusOK {
		panic("tagged child call did not receive a reply")
	}
	if tag := hostMessageGetTag(); tag != replyTag {
		panic("observer received an unexpected reply tag")
	}
	if value := readInt64Message(); value != replyTag {
		panic("tagged 41 -> 42 round trip returned an unexpected value")
	}

	if observerID != 0 {
		notifyObserver(observerID, completeTag)
	}
	println("GO_GUEST_E2E_OK")
}

func main() {
	if len(os.Args) >= 1 && os.Args[0] == "child" {
		if len(os.Args) != 2 {
			panic("child requires its root process ID in argv[1]")
		}
		rootID, err := strconv.ParseUint(os.Args[1], 10, 64)
		if err != nil || rootID == 0 {
			panic("child received an invalid root process ID")
		}
		runChild(rootID)
		return
	}

	// The host integration harness supplies an observer process ID as argv[0].
	// A direct `lunatic run` has a filename there instead. It uses observer ID
	// zero, performs the same internal assertions, and skips the final external
	// notification.
	var observerID uint64
	if len(os.Args) >= 1 {
		if supplied, err := strconv.ParseUint(os.Args[0], 10, 64); err == nil && supplied != 0 {
			observerID = supplied
		}
	}
	runRoot(observerID)
}
