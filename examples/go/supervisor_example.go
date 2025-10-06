package main

import (
	"fmt"
	"sync"
	"time"
)

// Supervisor pattern implementation in Go for Lunatic
// This demonstrates OTP-style process supervision with restart strategies

// Restart strategy types
type RestartStrategy string

const (
	OneForOne       RestartStrategy = "one_for_one"
	OneForAll       RestartStrategy = "one_for_all"
	RestForOne      RestartStrategy = "rest_for_one"
	SimpleOneForOne RestartStrategy = "simple_one_for_one"
)

// Restart policy types
type RestartPolicy string

const (
	Permanent RestartPolicy = "permanent"
	Transient RestartPolicy = "transient"
	Temporary RestartPolicy = "temporary"
)

// Shutdown policy types
type ShutdownPolicy string

const (
	Brutal   ShutdownPolicy = "brutal"
	Timeout  ShutdownPolicy = "timeout"
	Infinity ShutdownPolicy = "infinity"
)

// Child specification
type ChildSpec struct {
	ID        string
	Start     func() (*WorkerHandle, error)
	Restart   RestartPolicy
	Shutdown  ShutdownPolicy
	ChildType string
}

// Supervisor specification
type SupervisorSpec struct {
	Strategy    RestartStrategy
	MaxRestarts int
	MaxSeconds  int
	Children    []ChildSpec
}

// Supervisor state
type Supervisor struct {
	spec         SupervisorSpec
	children     map[string]*WorkerHandle
	restartCount int
	lastRestart  time.Time
	mu           sync.RWMutex
}

// Worker process simulation
type WorkerHandle struct {
	id       string
	running  bool
	restarts int
	stopCh   chan struct{}
	mu       sync.RWMutex
}

// Create a new worker
func newWorker(id string) *WorkerHandle {
	return &WorkerHandle{
		id:      id,
		running: false,
		stopCh:  make(chan struct{}),
	}
}

// Start the worker
func (w *WorkerHandle) start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		return fmt.Errorf("worker %s already running", w.id)
	}

	w.running = true
	go w.run()
	return nil
}

// Stop the worker
func (w *WorkerHandle) stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.running {
		return nil
	}

	w.running = false
	close(w.stopCh)
	return nil
}

// Worker main loop (simulates work)
func (w *WorkerHandle) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Simulate work
			fmt.Printf("Worker %s is working...\n", w.id)
		case <-w.stopCh:
			fmt.Printf("Worker %s stopped\n", w.id)
			return
		}
	}
}

// Check if worker is running
func (w *WorkerHandle) isRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.running
}

// Get worker info
func (w *WorkerHandle) info() map[string]interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return map[string]interface{}{
		"id":       w.id,
		"running":  w.running,
		"restarts": w.restarts,
	}
}

// Create a new supervisor
func newSupervisor(spec SupervisorSpec) *Supervisor {
	return &Supervisor{
		spec:     spec,
		children: make(map[string]*WorkerHandle),
	}
}

// Start all children
func (s *Supervisor) startChildren() error {
	for _, childSpec := range s.spec.Children {
		if err := s.startChild(childSpec.ID); err != nil {
			return fmt.Errorf("failed to start child %s: %v", childSpec.ID, err)
		}
	}
	return nil
}

// Start a specific child
func (s *Supervisor) startChild(childID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find child spec
	var childSpec *ChildSpec
	for _, spec := range s.spec.Children {
		if spec.ID == childID {
			childSpec = &spec
			break
		}
	}
	_ = childSpec // Mark as used

	if childSpec == nil {
		return fmt.Errorf("child %s not found", childID)
	}

	// Create and start worker
	worker := newWorker(childID)
	if err := worker.start(); err != nil {
		return err
	}

	s.children[childID] = worker
	return nil
}

// Handle child termination
func (s *Supervisor) handleChildExit(childID string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	child, exists := s.children[childID]
	if !exists {
		return fmt.Errorf("child %s not found", childID)
	}
	_ = child // Mark as used

	// Check if restart is needed
	shouldRestart := s.shouldRestart(childID, reason)
	if !shouldRestart {
		delete(s.children, childID)
		return nil
	}

	// Check restart intensity
	if !s.checkRestartIntensity() {
		return fmt.Errorf("restart intensity limit exceeded")
	}

	// Apply restart strategy
	switch s.spec.Strategy {
	case OneForOne:
		return s.restartChild(childID)
	case OneForAll:
		return s.restartAllChildren()
	case RestForOne:
		return s.restartFromChild(childID)
	default:
		return s.restartChild(childID)
	}
}

// Check if child should be restarted
func (s *Supervisor) shouldRestart(childID, reason string) bool {
	var childSpec *ChildSpec
	for _, spec := range s.spec.Children {
		if spec.ID == childID {
			childSpec = &spec
			break
		}
	}

	if childSpec == nil {
		return false
	}

	switch childSpec.Restart {
	case Permanent:
		return true
	case Transient:
		return reason != "normal"
	case Temporary:
		return false
	default:
		return false
	}
}

// Check restart intensity limit
func (s *Supervisor) checkRestartIntensity() bool {
	now := time.Now()

	// Reset counter if window has passed
	if now.Sub(s.lastRestart) > time.Duration(s.spec.MaxSeconds)*time.Second {
		s.restartCount = 0
	}

	s.restartCount++
	s.lastRestart = now

	return s.restartCount <= s.spec.MaxRestarts
}

// Restart a specific child
func (s *Supervisor) restartChild(childID string) error {
	// Stop existing child
	if child, exists := s.children[childID]; exists {
		child.stop()
		child.restarts++
	}

	// Start new child
	return s.startChild(childID)
}

// Restart all children
func (s *Supervisor) restartAllChildren() error {
	childIDs := make([]string, 0, len(s.children))
	for id := range s.children {
		childIDs = append(childIDs, id)
	}

	// Stop all children
	for _, id := range childIDs {
		if child, exists := s.children[id]; exists {
			child.stop()
			child.restarts++
		}
	}

	// Start all children
	for _, id := range childIDs {
		if err := s.startChild(id); err != nil {
			return err
		}
	}

	return nil
}

// Restart child and all children started after it
func (s *Supervisor) restartFromChild(failedChildID string) error {
	// Find index of failed child
	failedIndex := -1
	for i, spec := range s.spec.Children {
		if spec.ID == failedChildID {
			failedIndex = i
			break
		}
	}

	if failedIndex == -1 {
		return fmt.Errorf("child %s not found", failedChildID)
	}

	// Collect children to restart
	childrenToRestart := make([]string, 0)
	for i := failedIndex; i < len(s.spec.Children); i++ {
		childrenToRestart = append(childrenToRestart, s.spec.Children[i].ID)
	}

	// Stop children
	for _, id := range childrenToRestart {
		if child, exists := s.children[id]; exists {
			child.stop()
			child.restarts++
		}
	}

	// Start children
	for _, id := range childrenToRestart {
		if err := s.startChild(id); err != nil {
			return err
		}
	}

	return nil
}

// Get children information
func (s *Supervisor) whichChildren() []map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]map[string]interface{}, 0, len(s.children))
	for _, child := range s.children {
		result = append(result, child.info())
	}
	return result
}

// Count children
func (s *Supervisor) countChildren() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	specs := len(s.spec.Children)
	active := 0
	supervisors := 0
	workers := 0

	for _, child := range s.children {
		if child.isRunning() {
			active++
		}
	}

	for _, spec := range s.spec.Children {
		if spec.ChildType == "supervisor" {
			supervisors++
		} else {
			workers++
		}
	}

	return map[string]int{
		"specs":       specs,
		"active":      active,
		"supervisors": supervisors,
		"workers":     workers,
	}
}

// Stop the supervisor
func (s *Supervisor) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, child := range s.children {
		child.stop()
	}
	s.children = make(map[string]*WorkerHandle)
}

//export run_supervisor_example
func run_supervisor_example() {
	fmt.Println("Running Go Supervisor Example...")

	// Create supervisor spec
	spec := SupervisorSpec{
		Strategy:    OneForOne,
		MaxRestarts: 3,
		MaxSeconds:  5,
		Children: []ChildSpec{
			{
				ID:        "worker1",
				Start:     func() (*WorkerHandle, error) { return newWorker("worker1"), nil },
				Restart:   Permanent,
				Shutdown:  Timeout,
				ChildType: "worker",
			},
			{
				ID:        "worker2",
				Start:     func() (*WorkerHandle, error) { return newWorker("worker2"), nil },
				Restart:   Transient,
				Shutdown:  Brutal,
				ChildType: "worker",
			},
		},
	}

	// Create and start supervisor
	supervisor := newSupervisor(spec)
	defer supervisor.stop()

	if err := supervisor.startChildren(); err != nil {
		fmt.Printf("Failed to start children: %v\n", err)
		return
	}

	// Check initial state
	count := supervisor.countChildren()
	fmt.Printf("Initial count - specs: %d, active: %d\n", count["specs"], count["active"])

	children := supervisor.whichChildren()
	fmt.Printf("Children: %d\n", len(children))
	for _, child := range children {
		fmt.Printf("  %s: running=%v, restarts=%d\n",
			child["id"], child["running"], child["restarts"])
	}

	// Simulate child failure
	fmt.Println("Simulating worker1 failure...")
	if err := supervisor.handleChildExit("worker1", "crash"); err != nil {
		fmt.Printf("Failed to handle child exit: %v\n", err)
	}

	// Check state after restart
	time.Sleep(100 * time.Millisecond)
	count = supervisor.countChildren()
	fmt.Printf("After restart - specs: %d, active: %d\n", count["specs"], count["active"])

	children = supervisor.whichChildren()
	fmt.Printf("Children after restart: %d\n", len(children))
	for _, child := range children {
		fmt.Printf("  %s: running=%v, restarts=%d\n",
			child["id"], child["running"], child["restarts"])
	}

	fmt.Println("Go Supervisor example completed successfully!")
}

//export _start
func _start() {
	run_supervisor_example()
}
