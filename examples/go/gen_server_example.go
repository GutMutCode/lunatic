package main

import (
	"fmt"
)

// GenServer pattern implementation in Go for Lunatic
// This demonstrates OTP-style message passing using channels

// Message types for GenServer communication
type CounterRequest struct {
	Type  string
	Value int
	Reply chan CounterResponse
}

type CounterResponse struct {
	Type  string
	Value int
}

// Counter GenServer state
type CounterServer struct {
	count int
}

// Create a new counter server
func newCounterServer() *CounterServer {
	return &CounterServer{count: 0}
}

// Handle synchronous calls (like GenServer.call)
func (c *CounterServer) handleCall(request CounterRequest) CounterResponse {
	switch request.Type {
	case "increment":
		c.count++
		return CounterResponse{Type: "ok", Value: c.count}
	case "decrement":
		c.count--
		return CounterResponse{Type: "ok", Value: c.count}
	case "get":
		return CounterResponse{Type: "value", Value: c.count}
	case "set":
		c.count = request.Value
		return CounterResponse{Type: "ok", Value: c.count}
	default:
		return CounterResponse{Type: "error", Value: 0}
	}
}

// Handle asynchronous casts (like GenServer.cast)
func (c *CounterServer) handleCast(request CounterRequest) {
	switch request.Type {
	case "increment":
		c.count++
	case "decrement":
		c.count--
	case "set":
		c.count = request.Value
		// "get" cast is ignored (no response)
	}
}

// GenServer main loop
func (c *CounterServer) run(requests <-chan CounterRequest) {
	for request := range requests {
		if request.Reply != nil {
			// Synchronous call
			response := c.handleCall(request)
			request.Reply <- response
		} else {
			// Asynchronous cast
			c.handleCast(request)
		}
	}
}

// GenServer handle for external communication
type CounterHandle struct {
	requests chan<- CounterRequest
}

// Create a new GenServer instance
func spawnCounterServer() *CounterHandle {
	server := newCounterServer()
	requests := make(chan CounterRequest, 10)

	go server.run(requests)

	return &CounterHandle{requests: requests}
}

// Synchronous call (blocks until response)
func (h *CounterHandle) call(requestType string, value int) CounterResponse {
	reply := make(chan CounterResponse, 1)
	request := CounterRequest{
		Type:  requestType,
		Value: value,
		Reply: reply,
	}

	h.requests <- request
	return <-reply
}

// Asynchronous cast (fire and forget)
func (h *CounterHandle) cast(requestType string, value int) {
	request := CounterRequest{
		Type:  requestType,
		Value: value,
		Reply: nil,
	}

	h.requests <- request
}

// Stop the GenServer
func (h *CounterHandle) stop() {
	close(h.requests)
}

//export run_gen_server_example
func run_gen_server_example() {
	fmt.Println("Running Go GenServer Example...")

	// Spawn a counter server
	handle := spawnCounterServer()
	defer handle.stop()

	// Test synchronous calls
	fmt.Println("Initial value:", handle.call("get", 0).Value)

	handle.call("increment", 0)
	fmt.Println("After increment:", handle.call("get", 0).Value)

	handle.call("set", 42)
	fmt.Println("After set to 42:", handle.call("get", 0).Value)

	handle.call("decrement", 0)
	fmt.Println("After decrement:", handle.call("get", 0).Value)

	// Test asynchronous casts
	handle.cast("increment", 0)
	handle.cast("increment", 0)
	fmt.Println("After 2 casts:", handle.call("get", 0).Value)

	fmt.Println("Go GenServer example completed successfully!")
}

//export _start
func _start() {
	run_gen_server_example()
}

func main() {
	// For testing outside WASM
	run_gen_server_example()
}
