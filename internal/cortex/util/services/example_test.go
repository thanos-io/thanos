package services

import (
	"context"
	"fmt"
)

type exampleService struct {
	*BasicService

	log []string
	ch  chan string
}

func newExampleServ() *exampleService {
	s := &exampleService{
		ch: make(chan string),
	}
	s.BasicService = NewBasicService(nil, s.collect, nil) // StartingFn, RunningFn, StoppingFn
	return s
}

// used as Running function. When service is stopped, context is canceled, so we react on it.
func (s *exampleService) collect(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-s.ch:
			s.log = append(s.log, msg)
		}
	}
}

// External method called by clients of the Service.
func (s *exampleService) Send(msg string) bool {
	ctx := s.ServiceContext()
	if ctx == nil {
		// Service is not yet started
		return false
	}
	select {
	case s.ch <- msg:
		return true
	case <-ctx.Done():
		// Service is not running anymore.
		return false
	}
}

func ExampleService() {
	es := newExampleServ()
	es.Send("first") // ignored, as service is not running yet

	_ = es.StartAsync(context.Background())
	_ = es.AwaitRunning(context.Background())

	es.Send("second")

	es.StopAsync()
	_ = es.AwaitTerminated(context.Background())

	es.Send("third") // ignored, service is now stopped

	fmt.Println(es.log)
	// Output: [second]
}
