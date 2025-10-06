package main

import "fmt"

// Global state
var counter int32 = 0

// _start is called when the module is initialized
//
//export _start
func _start() {
	counter = 0
	fmt.Println("Go Counter initialized!")
}

// increment increases the counter and returns the new value
//
//export increment
func increment() int32 {
	counter++
	return counter
}

// get_count returns the current counter value
//
//export get_count
func get_count() int32 {
	return counter
}

// reset sets the counter back to zero
//
//export reset
func reset() {
	counter = 0
}

// set_count sets the counter to a specific value
//
//export set_count
func set_count(value int32) {
	counter = value
}

// multiply_count multiplies the counter by a factor
//
//export multiply_count
func multiply_count(factor int32) int32 {
	counter *= factor
	return counter
}

// is_even checks if the counter is even
//
//export is_even
func is_even() bool {
	return counter%2 == 0
}

// Required main function for TinyGo
func main() {
	// Main is required but not used in WASM modules
	// All initialization happens in _start()
}
