package controller

type SchedulerRepository interface {
	EnqueueExecution()
}

type Scheduler struct {
}

func NewScheduler() SchedulerRepository {
	return &Scheduler{}
}

func (s Scheduler) EnqueueExecution() {

}