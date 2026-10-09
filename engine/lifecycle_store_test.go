package engine_test

// This fixture checks every exported memory store method, including optional capabilities.
import (
	context "context"
	time "time"

	artifact "github.com/xraph/dispatch/artifact"
	cluster "github.com/xraph/dispatch/cluster"
	cron "github.com/xraph/dispatch/cron"
	dlq "github.com/xraph/dispatch/dlq"
	durable "github.com/xraph/dispatch/durable"
	event "github.com/xraph/dispatch/event"
	id "github.com/xraph/dispatch/id"
	job "github.com/xraph/dispatch/job"
	workflow "github.com/xraph/dispatch/workflow"
)

func (s *strictLifecycleStore) AckEvent(arg0 context.Context, arg1 id.EventID) error {
	defer s.call("AckEvent")()
	return s.base.AckEvent(arg0, arg1)
}
func (s *strictLifecycleStore) AcknowledgeDelivery(arg0 context.Context, arg1 durable.DeliveryToken, arg2 durable.SinkReceipt) error {
	defer s.call("AcknowledgeDelivery")()
	return s.base.AcknowledgeDelivery(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) AcquireCronLock(arg0 context.Context, arg1 id.CronID, arg2 id.WorkerID, arg3 time.Duration) (bool, error) {
	defer s.call("AcquireCronLock")()
	return s.base.AcquireCronLock(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) AcquireLeadership(arg0 context.Context, arg1 id.WorkerID, arg2 time.Duration) (bool, error) {
	defer s.call("AcquireLeadership")()
	return s.base.AcquireLeadership(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) AppendSecurityAudit(arg0 context.Context, arg1 durable.SecurityAudit) (durable.Delivery, error) {
	defer s.call("AppendSecurityAudit")()
	return s.base.AppendSecurityAudit(arg0, arg1)
}
func (s *strictLifecycleStore) ApplyChildDelivery(arg0 context.Context, arg1 durable.ChildDeliveryRequest) (durable.ChildDeliveryReceipt, error) {
	defer s.call("ApplyChildDelivery")()
	return s.base.ApplyChildDelivery(arg0, arg1)
}
func (s *strictLifecycleStore) ApplyExecutionTimeout(arg0 context.Context, arg1 durable.ExecutionTimeoutRequest) (durable.Receipt, error) {
	defer s.call("ApplyExecutionTimeout")()
	return s.base.ApplyExecutionTimeout(arg0, arg1)
}
func (s *strictLifecycleStore) BeginLegacyAttempt(arg0 context.Context, arg1 durable.SecurityAudit) (durable.LegacyAttempt, error) {
	defer s.call("BeginLegacyAttempt")()
	return s.base.BeginLegacyAttempt(arg0, arg1)
}
func (s *strictLifecycleStore) ClaimChildDelivery(arg0 context.Context, arg1 durable.ChildDeliveryClaimRequest) (*durable.ChildDelivery, error) {
	defer s.call("ClaimChildDelivery")()
	return s.base.ClaimChildDelivery(arg0, arg1)
}
func (s *strictLifecycleStore) ClaimDeliveries(arg0 context.Context, arg1 durable.DeliveryClaim) ([]durable.DeliveryRecord, error) {
	defer s.call("ClaimDeliveries")()
	return s.base.ClaimDeliveries(arg0, arg1)
}
func (s *strictLifecycleStore) ClaimExecutionTimeout(arg0 context.Context, arg1 durable.ExecutionTimeoutClaimRequest) (*durable.ExecutionTimeoutTask, error) {
	defer s.call("ClaimExecutionTimeout")()
	return s.base.ClaimExecutionTimeout(arg0, arg1)
}
func (s *strictLifecycleStore) ClaimReplay(arg0 context.Context, arg1 id.DLQID, arg2 id.JobID) error {
	defer s.call("ClaimReplay")()
	return s.base.ClaimReplay(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) ClaimTask(arg0 context.Context, arg1 durable.ClaimRequest) (*durable.Task, error) {
	defer s.call("ClaimTask")()
	return s.base.ClaimTask(arg0, arg1)
}
func (s *strictLifecycleStore) ClaimTimeoutTask(arg0 context.Context, arg1 durable.TimeoutClaimRequest) (*durable.Task, error) {
	defer s.call("ClaimTimeoutTask")()
	return s.base.ClaimTimeoutTask(arg0, arg1)
}
func (s *strictLifecycleStore) CommitTransition(arg0 context.Context, arg1 durable.CommitRequest) (durable.Receipt, error) {
	defer s.call("CommitTransition")()
	return s.base.CommitTransition(arg0, arg1)
}
func (s *strictLifecycleStore) CompleteLegacyAttempt(arg0 context.Context, arg1 durable.LegacyOutcome) error {
	defer s.call("CompleteLegacyAttempt")()
	return s.base.CompleteLegacyAttempt(arg0, arg1)
}
func (s *strictLifecycleStore) CountDLQ(arg0 context.Context) (int64, error) {
	defer s.call("CountDLQ")()
	return s.base.CountDLQ(arg0)
}
func (s *strictLifecycleStore) CountDLQEntries(arg0 context.Context, arg1 dlq.CountOpts) (int64, error) {
	defer s.call("CountDLQEntries")()
	return s.base.CountDLQEntries(arg0, arg1)
}
func (s *strictLifecycleStore) CountJobs(arg0 context.Context, arg1 job.CountOpts) (int64, error) {
	defer s.call("CountJobs")()
	return s.base.CountJobs(arg0, arg1)
}
func (s *strictLifecycleStore) CountRuns(arg0 context.Context, arg1 workflow.CountRunsOpts) (int64, error) {
	defer s.call("CountRuns")()
	return s.base.CountRuns(arg0, arg1)
}
func (s *strictLifecycleStore) CreateArtifact(arg0 context.Context, arg1 *artifact.Artifact, arg2 *artifact.Link) error {
	defer s.call("CreateArtifact")()
	return s.base.CreateArtifact(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) CreateRun(arg0 context.Context, arg1 *workflow.Run) error {
	defer s.call("CreateRun")()
	return s.base.CreateRun(arg0, arg1)
}
func (s *strictLifecycleStore) DeleteCheckpointsAfter(arg0 context.Context, arg1 id.RunID, arg2 string) error {
	defer s.call("DeleteCheckpointsAfter")()
	return s.base.DeleteCheckpointsAfter(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) DeleteCron(arg0 context.Context, arg1 id.CronID) error {
	defer s.call("DeleteCron")()
	return s.base.DeleteCron(arg0, arg1)
}
func (s *strictLifecycleStore) DeleteDLQ(arg0 context.Context, arg1 id.DLQID) error {
	defer s.call("DeleteDLQ")()
	return s.base.DeleteDLQ(arg0, arg1)
}
func (s *strictLifecycleStore) DeleteJob(arg0 context.Context, arg1 id.JobID) error {
	defer s.call("DeleteJob")()
	return s.base.DeleteJob(arg0, arg1)
}
func (s *strictLifecycleStore) DeleteStaleWorkers(arg0 context.Context, arg1 time.Duration) (int64, error) {
	defer s.call("DeleteStaleWorkers")()
	return s.base.DeleteStaleWorkers(arg0, arg1)
}
func (s *strictLifecycleStore) DeliveryStatus(arg0 context.Context, arg1 durable.DeliveryStatusRequest) (durable.DeliveryStatus, error) {
	defer s.call("DeliveryStatus")()
	return s.base.DeliveryStatus(arg0, arg1)
}
func (s *strictLifecycleStore) DequeueJobs(arg0 context.Context, arg1 job.DequeueOpts) ([]*job.Job, error) {
	defer s.call("DequeueJobs")()
	return s.base.DequeueJobs(arg0, arg1)
}
func (s *strictLifecycleStore) DeregisterWorker(arg0 context.Context, arg1 id.WorkerID) error {
	defer s.call("DeregisterWorker")()
	return s.base.DeregisterWorker(arg0, arg1)
}
func (s *strictLifecycleStore) EnqueueJob(arg0 context.Context, arg1 *job.Job) error {
	defer s.call("EnqueueJob")()
	return s.base.EnqueueJob(arg0, arg1)
}
func (s *strictLifecycleStore) FindArtifactByKey(arg0 context.Context, arg1, arg2, arg3 string) (*artifact.Artifact, error) {
	defer s.call("FindArtifactByKey")()
	return s.base.FindArtifactByKey(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) FindLinkByName(arg0 context.Context, arg1 artifact.OwnerRef, arg2 string) (*artifact.Link, error) {
	defer s.call("FindLinkByName")()
	return s.base.FindLinkByName(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) GetArtifact(arg0 context.Context, arg1 id.ArtifactID) (*artifact.Artifact, error) {
	defer s.call("GetArtifact")()
	return s.base.GetArtifact(arg0, arg1)
}
func (s *strictLifecycleStore) GetArtifactRecord(arg0 context.Context, arg1 id.ArtifactID) (*artifact.Artifact, error) {
	defer s.call("GetArtifactRecord")()
	return s.base.GetArtifactRecord(arg0, arg1)
}
func (s *strictLifecycleStore) GetCheckpoint(arg0 context.Context, arg1 id.RunID, arg2 string) ([]byte, error) {
	defer s.call("GetCheckpoint")()
	return s.base.GetCheckpoint(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) GetChildDelivery(arg0 context.Context, arg1 durable.Key, arg2 string) (durable.ChildDelivery, error) {
	defer s.call("GetChildDelivery")()
	return s.base.GetChildDelivery(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) GetChildExecution(arg0 context.Context, arg1 durable.Key, arg2 string) (durable.ChildExecution, error) {
	defer s.call("GetChildExecution")()
	return s.base.GetChildExecution(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) GetCron(arg0 context.Context, arg1 id.CronID) (*cron.Entry, error) {
	defer s.call("GetCron")()
	return s.base.GetCron(arg0, arg1)
}
func (s *strictLifecycleStore) GetDLQ(arg0 context.Context, arg1 id.DLQID) (*dlq.Entry, error) {
	defer s.call("GetDLQ")()
	return s.base.GetDLQ(arg0, arg1)
}
func (s *strictLifecycleStore) GetDLQByJobID(arg0 context.Context, arg1 id.JobID) (*dlq.Entry, error) {
	defer s.call("GetDLQByJobID")()
	return s.base.GetDLQByJobID(arg0, arg1)
}
func (s *strictLifecycleStore) GetExecution(arg0 context.Context, arg1 durable.Key) (durable.Execution, error) {
	defer s.call("GetExecution")()
	return s.base.GetExecution(arg0, arg1)
}
func (s *strictLifecycleStore) GetJob(arg0 context.Context, arg1 id.JobID) (*job.Job, error) {
	defer s.call("GetJob")()
	return s.base.GetJob(arg0, arg1)
}
func (s *strictLifecycleStore) GetLeader(arg0 context.Context) (*cluster.Worker, error) {
	defer s.call("GetLeader")()
	return s.base.GetLeader(arg0)
}
func (s *strictLifecycleStore) GetNamespace(arg0 context.Context, arg1, arg2 string) (durable.NamespaceRecord, error) {
	defer s.call("GetNamespace")()
	return s.base.GetNamespace(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) GetParentExecution(arg0 context.Context, arg1 durable.Key) (durable.ChildExecution, error) {
	defer s.call("GetParentExecution")()
	return s.base.GetParentExecution(arg0, arg1)
}
func (s *strictLifecycleStore) GetRun(arg0 context.Context, arg1 id.RunID) (*workflow.Run, error) {
	defer s.call("GetRun")()
	return s.base.GetRun(arg0, arg1)
}
func (s *strictLifecycleStore) GetTask(arg0 context.Context, arg1 durable.Key, arg2 string) (durable.Task, error) {
	defer s.call("GetTask")()
	return s.base.GetTask(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) GetWorker(arg0 context.Context, arg1 id.WorkerID) (*cluster.Worker, error) {
	defer s.call("GetWorker")()
	return s.base.GetWorker(arg0, arg1)
}
func (s *strictLifecycleStore) HeartbeatJob(arg0 context.Context, arg1 id.JobID, arg2 id.WorkerID) error {
	defer s.call("HeartbeatJob")()
	return s.base.HeartbeatJob(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) HeartbeatWorker(arg0 context.Context, arg1 id.WorkerID) error {
	defer s.call("HeartbeatWorker")()
	return s.base.HeartbeatWorker(arg0, arg1)
}
func (s *strictLifecycleStore) LinkArtifact(arg0 context.Context, arg1 *artifact.Link) error {
	defer s.call("LinkArtifact")()
	return s.base.LinkArtifact(arg0, arg1)
}
func (s *strictLifecycleStore) ListArtifacts(arg0 context.Context, arg1 artifact.ListOpts) ([]*artifact.Artifact, error) {
	defer s.call("ListArtifacts")()
	return s.base.ListArtifacts(arg0, arg1)
}
func (s *strictLifecycleStore) ListArtifactsByOwner(arg0 context.Context, arg1 artifact.OwnerRef, arg2 artifact.Role,
) ([]*artifact.Artifact, error) {
	defer s.call("ListArtifactsByOwner")()
	return s.base.ListArtifactsByOwner(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) ListArtifactsPage(arg0 context.Context, arg1 artifact.PageOpts) (artifact.Page, error) {
	defer s.call("ListArtifactsPage")()
	return s.base.ListArtifactsPage(arg0, arg1)
}
func (s *strictLifecycleStore) ListCheckpoints(arg0 context.Context, arg1 id.RunID) ([]*workflow.Checkpoint, error) {
	defer s.call("ListCheckpoints")()
	return s.base.ListCheckpoints(arg0, arg1)
}
func (s *strictLifecycleStore) ListChildDeliveries(arg0 context.Context, arg1 durable.Key, arg2 string, arg3 int) ([]durable.ChildDelivery, error) {
	defer s.call("ListChildDeliveries")()
	return s.base.ListChildDeliveries(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) ListChildExecutions(arg0 context.Context, arg1 durable.Key, arg2 string, arg3 int) ([]durable.ChildExecution, error) {
	defer s.call("ListChildExecutions")()
	return s.base.ListChildExecutions(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) ListChildRuns(arg0 context.Context, arg1 id.RunID) ([]*workflow.Run, error) {
	defer s.call("ListChildRuns")()
	return s.base.ListChildRuns(arg0, arg1)
}
func (s *strictLifecycleStore) ListCrons(arg0 context.Context) ([]*cron.Entry, error) {
	defer s.call("ListCrons")()
	return s.base.ListCrons(arg0)
}
func (s *strictLifecycleStore) ListDLQ(arg0 context.Context, arg1 dlq.ListOpts) ([]*dlq.Entry, error) {
	defer s.call("ListDLQ")()
	return s.base.ListDLQ(arg0, arg1)
}
func (s *strictLifecycleStore) ListDLQPage(arg0 context.Context, arg1 dlq.PageOpts) (dlq.Page, error) {
	defer s.call("ListDLQPage")()
	return s.base.ListDLQPage(arg0, arg1)
}
func (s *strictLifecycleStore) ListJobs(arg0 context.Context, arg1 job.ListJobsOpts) (job.Page, error) {
	defer s.call("ListJobs")()
	return s.base.ListJobs(arg0, arg1)
}
func (s *strictLifecycleStore) ListJobsByState(arg0 context.Context, arg1 job.State, arg2 job.ListOpts) ([]*job.Job, error) {
	defer s.call("ListJobsByState")()
	return s.base.ListJobsByState(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) ListLinks(arg0 context.Context, arg1 artifact.OwnerRef) ([]*artifact.Link, error) {
	defer s.call("ListLinks")()
	return s.base.ListLinks(arg0, arg1)
}
func (s *strictLifecycleStore) ListNamespaces(arg0 context.Context, arg1 durable.NamespaceList) ([]durable.NamespaceRecord, error) {
	defer s.call("ListNamespaces")()
	return s.base.ListNamespaces(arg0, arg1)
}
func (s *strictLifecycleStore) ListPurgeable(arg0 context.Context, arg1 time.Duration, arg2 int) ([]*artifact.Artifact, error) {
	defer s.call("ListPurgeable")()
	return s.base.ListPurgeable(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) ListRuns(arg0 context.Context, arg1 workflow.ListOpts) ([]*workflow.Run, error) {
	defer s.call("ListRuns")()
	return s.base.ListRuns(arg0, arg1)
}
func (s *strictLifecycleStore) ListRunsPage(arg0 context.Context, arg1 workflow.ListRunsPageOpts) (workflow.RunPage, error) {
	defer s.call("ListRunsPage")()
	return s.base.ListRunsPage(arg0, arg1)
}
func (s *strictLifecycleStore) ListWorkers(arg0 context.Context) ([]*cluster.Worker, error) {
	defer s.call("ListWorkers")()
	return s.base.ListWorkers(arg0)
}
func (s *strictLifecycleStore) LookupReceipt(arg0 context.Context, arg1 durable.ReceiptRequest) (durable.Receipt, bool, error) {
	defer s.call("LookupReceipt")()
	return s.base.LookupReceipt(arg0, arg1)
}
func (s *strictLifecycleStore) Migrate(arg0 context.Context) error {
	defer s.call("Migrate")()
	return s.base.Migrate(arg0)
}
func (s *strictLifecycleStore) Ping(arg0 context.Context) error {
	defer s.call("Ping")()
	return s.base.Ping(arg0)
}
func (s *strictLifecycleStore) PublishEvent(arg0 context.Context, arg1 *event.Event) error {
	defer s.call("PublishEvent")()
	return s.base.PublishEvent(arg0, arg1)
}
func (s *strictLifecycleStore) PurgeArtifact(arg0 context.Context, arg1 id.ArtifactID) error {
	defer s.call("PurgeArtifact")()
	return s.base.PurgeArtifact(arg0, arg1)
}
func (s *strictLifecycleStore) PurgeDLQ(arg0 context.Context, arg1 time.Time) (int64, error) {
	defer s.call("PurgeDLQ")()
	return s.base.PurgeDLQ(arg0, arg1)
}
func (s *strictLifecycleStore) PushDLQ(arg0 context.Context, arg1 *dlq.Entry) error {
	defer s.call("PushDLQ")()
	return s.base.PushDLQ(arg0, arg1)
}
func (s *strictLifecycleStore) ReadHistory(arg0 context.Context, arg1 durable.Key, arg2 int64, arg3 int) ([]durable.Event, error) {
	defer s.call("ReadHistory")()
	return s.base.ReadHistory(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) ReapDeadWorkers(arg0 context.Context, arg1 time.Duration) ([]*cluster.Worker, error) {
	defer s.call("ReapDeadWorkers")()
	return s.base.ReapDeadWorkers(arg0, arg1)
}
func (s *strictLifecycleStore) ReapStaleJobs(arg0 context.Context, arg1 time.Duration) ([]*job.Job, error) {
	defer s.call("ReapStaleJobs")()
	return s.base.ReapStaleJobs(arg0, arg1)
}
func (s *strictLifecycleStore) ReclaimExpiredLeases(arg0 context.Context, arg1 int) ([]*job.Job, error) {
	defer s.call("ReclaimExpiredLeases")()
	return s.base.ReclaimExpiredLeases(arg0, arg1)
}
func (s *strictLifecycleStore) RecordHeartbeat(arg0 context.Context, arg1 durable.HeartbeatRequest) (durable.Receipt, error) {
	defer s.call("RecordHeartbeat")()
	return s.base.RecordHeartbeat(arg0, arg1)
}
func (s *strictLifecycleStore) RegisterCron(arg0 context.Context, arg1 *cron.Entry) error {
	defer s.call("RegisterCron")()
	return s.base.RegisterCron(arg0, arg1)
}
func (s *strictLifecycleStore) RegisterNamespace(arg0 context.Context, arg1 durable.NamespaceConfig) (durable.NamespaceRecord, error) {
	defer s.call("RegisterNamespace")()
	return s.base.RegisterNamespace(arg0, arg1)
}
func (s *strictLifecycleStore) RegisterWorker(arg0 context.Context, arg1 *cluster.Worker) error {
	defer s.call("RegisterWorker")()
	return s.base.RegisterWorker(arg0, arg1)
}
func (s *strictLifecycleStore) ReleaseCronLock(arg0 context.Context, arg1 id.CronID, arg2 id.WorkerID) error {
	defer s.call("ReleaseCronLock")()
	return s.base.ReleaseCronLock(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) ReleaseReplay(arg0 context.Context, arg1 id.DLQID, arg2 id.JobID) error {
	defer s.call("ReleaseReplay")()
	return s.base.ReleaseReplay(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) RenewDelivery(arg0 context.Context, arg1 durable.DeliveryToken, arg2 time.Duration) (time.Time, error) {
	defer s.call("RenewDelivery")()
	return s.base.RenewDelivery(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) RenewLeadership(arg0 context.Context, arg1 id.WorkerID, arg2 time.Duration) (bool, error) {
	defer s.call("RenewLeadership")()
	return s.base.RenewLeadership(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) RenewLease(arg0 context.Context, arg1 id.JobID, arg2 id.WorkerID, arg3 int, arg4 time.Time,
) error {
	defer s.call("RenewLease")()
	return s.base.RenewLease(arg0, arg1, arg2, arg3, arg4)
}
func (s *strictLifecycleStore) RenewTask(arg0 context.Context, arg1 durable.Key, arg2 durable.TaskToken, arg3 time.Duration) (time.Time, error) {
	defer s.call("RenewTask")()
	return s.base.RenewTask(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) ReopenRun(arg0 context.Context, arg1 id.RunID, arg2 int64) error {
	defer s.call("ReopenRun")()
	return s.base.ReopenRun(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) ReplayDLQ(arg0 context.Context, arg1 id.DLQID) error {
	defer s.call("ReplayDLQ")()
	return s.base.ReplayDLQ(arg0, arg1)
}
func (s *strictLifecycleStore) RequestCancelExecution(arg0 context.Context, arg1 durable.CancelExecutionRequest) (durable.CancelExecutionReceipt, error) {
	defer s.call("RequestCancelExecution")()
	return s.base.RequestCancelExecution(arg0, arg1)
}
func (s *strictLifecycleStore) ResolveExecution(arg0 context.Context, arg1 durable.ExecutionTarget) (durable.Execution, error) {
	defer s.call("ResolveExecution")()
	return s.base.ResolveExecution(arg0, arg1)
}
func (s *strictLifecycleStore) RetryDelivery(arg0 context.Context, arg1 durable.DeliveryRetry) error {
	defer s.call("RetryDelivery")()
	return s.base.RetryDelivery(arg0, arg1)
}
func (s *strictLifecycleStore) SaveCheckpoint(arg0 context.Context, arg1 id.RunID, arg2 string, arg3 []byte) error {
	defer s.call("SaveCheckpoint")()
	return s.base.SaveCheckpoint(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) SetCronEnabled(arg0 context.Context, arg1 id.CronID, arg2 bool, arg3 *time.Time) error {
	defer s.call("SetCronEnabled")()
	return s.base.SetCronEnabled(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) SignalExecution(arg0 context.Context, arg1 durable.SignalRequest) (durable.SignalReceipt, error) {
	defer s.call("SignalExecution")()
	return s.base.SignalExecution(arg0, arg1)
}
func (s *strictLifecycleStore) SignalWithStart(arg0 context.Context, arg1 durable.SignalWithStartRequest) (durable.SignalReceipt, error) {
	defer s.call("SignalWithStart")()
	return s.base.SignalWithStart(arg0, arg1)
}
func (s *strictLifecycleStore) StartExecution(arg0 context.Context, arg1 durable.StartRequest) (durable.Receipt, error) {
	defer s.call("StartExecution")()
	return s.base.StartExecution(arg0, arg1)
}
func (s *strictLifecycleStore) SubscribeEvent(arg0 context.Context, arg1 string, arg2 time.Duration) (*event.Event, error) {
	defer s.call("SubscribeEvent")()
	return s.base.SubscribeEvent(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) SweepEphemeral(arg0 context.Context, arg1 artifact.SweepOpts) ([]*artifact.Artifact, error) {
	defer s.call("SweepEphemeral")()
	return s.base.SweepEphemeral(arg0, arg1)
}
func (s *strictLifecycleStore) SweepOrphans(arg0 context.Context, arg1 time.Time, arg2 int) ([]*artifact.Artifact, error) {
	defer s.call("SweepOrphans")()
	return s.base.SweepOrphans(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) UnresolvedLegacyAttempts(arg0 context.Context, arg1 durable.LegacyAttemptList) ([]durable.LegacyAttempt, error) {
	defer s.call("UnresolvedLegacyAttempts")()
	return s.base.UnresolvedLegacyAttempts(arg0, arg1)
}
func (s *strictLifecycleStore) UpdateArtifact(arg0 context.Context, arg1 *artifact.Artifact) error {
	defer s.call("UpdateArtifact")()
	return s.base.UpdateArtifact(arg0, arg1)
}
func (s *strictLifecycleStore) UpdateCronEntry(arg0 context.Context, arg1 *cron.Entry) error {
	defer s.call("UpdateCronEntry")()
	return s.base.UpdateCronEntry(arg0, arg1)
}
func (s *strictLifecycleStore) UpdateCronLastRun(arg0 context.Context, arg1 id.CronID, arg2 time.Time) error {
	defer s.call("UpdateCronLastRun")()
	return s.base.UpdateCronLastRun(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) UpdateCronNextRun(arg0 context.Context, arg1 id.CronID, arg2 time.Time) error {
	defer s.call("UpdateCronNextRun")()
	return s.base.UpdateCronNextRun(arg0, arg1, arg2)
}
func (s *strictLifecycleStore) UpdateJob(arg0 context.Context, arg1 *job.Job) error {
	defer s.call("UpdateJob")()
	return s.base.UpdateJob(arg0, arg1)
}
func (s *strictLifecycleStore) UpdateLeasedJob(arg0 context.Context, arg1 *job.Job, arg2 id.WorkerID, arg3 int) error {
	defer s.call("UpdateLeasedJob")()
	return s.base.UpdateLeasedJob(arg0, arg1, arg2, arg3)
}
func (s *strictLifecycleStore) UpdateRun(arg0 context.Context, arg1 *workflow.Run) error {
	defer s.call("UpdateRun")()
	return s.base.UpdateRun(arg0, arg1)
}

func (s *strictLifecycleStore) BlockDelivery(ctx context.Context, token durable.DeliveryToken) error {
	defer s.call("BlockDelivery")()
	return s.base.BlockDelivery(ctx, token)
}
