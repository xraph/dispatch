package operatorhost

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/dispatch/durable"
)

func seed(ctx context.Context, store Store) error {
	record, err := store.GetNamespace(ctx, "operator-host", "production")
	if err != nil {
		return err
	}
	// Private catalog candidates exercise incomplete discovery in the browser.
	for i := 0; i < 35; i++ {
		config := record.NamespaceConfig
		config.Namespace = fmt.Sprintf("000-private-%02d", i)
		config.TenantID = "tenant-private"
		if _, err = store.RegisterNamespace(ctx, config); err != nil {
			return err
		}
	}
	for _, namespace := range []string{"production", "foreign"} {
		for _, workflow := range []string{"invoice", "approval"} {
			key := durable.Key{Namespace: namespace, WorkflowID: workflow, RunID: "run-1"}
			request := durable.StartRequest{Key: key, RequestID: "fixture-start", WorkflowType: workflow, BuildID: "historical-v1", Queue: "fixture-" + workflow, Input: []byte(`{"fixture":"protected input"}`)}
			if _, err = store.StartExecution(ctx, request); err != nil {
				return err
			}
			if workflow != "invoice" {
				continue
			}
			execution, getErr := store.GetExecution(ctx, key)
			if getErr != nil {
				return getErr
			}
			if execution.State != durable.StateRunning {
				continue
			}
			task, claimErr := store.ClaimTask(ctx, durable.ClaimRequest{Namespace: namespace, Queue: request.Queue, Kind: durable.TaskWorkflow, BuildID: request.BuildID, Owner: "fixture-seed", LeaseDuration: time.Minute})
			if claimErr != nil {
				return claimErr
			}
			if task == nil {
				return fmt.Errorf("operator fixture: continuation task unavailable")
			}
			_, err = store.CommitTransition(ctx, durable.CommitRequest{Key: key, RequestID: "fixture-continue", ExpectedRevision: execution.Revision, Token: task.Token(), State: durable.StateContinuedAsNew, Events: []durable.EventInput{{Type: "workflow.waiting"}}, Continuation: &durable.ContinueSpec{RunID: "run-2", WorkflowType: workflow, BuildID: request.BuildID, Queue: request.Queue, Input: []byte(`{"fixture":"successor input"}`)}})
			if err != nil {
				return err
			}
		}
	}
	// These seeded conflicts exercise the projection, not remote sink delivery.
	for _, destination := range []durable.Destination{durable.DestinationChronicle, durable.DestinationRelay} {
		records, claimErr := store.ClaimDeliveries(ctx, durable.DeliveryClaim{DeliveryScope: durable.DeliveryScope{InstallationID: "operator-host", Destination: destination}, Owner: "fixture-seed", Limit: 100, LeaseDuration: time.Minute})
		if claimErr != nil {
			return claimErr
		}
		blocked := false
		for _, delivery := range records {
			if !blocked && delivery.Delivery.Namespace == "production" && delivery.Delivery.WorkflowID == "invoice" {
				if blockErr := store.BlockDelivery(ctx, delivery.Token()); blockErr != nil {
					return blockErr
				}
				blocked = true
				continue
			}
			if retryErr := store.RetryDelivery(ctx, durable.DeliveryRetry{Token: delivery.Token(), Category: "unavailable", Delay: time.Second}); retryErr != nil {
				return retryErr
			}
		}
	}
	return nil
}
