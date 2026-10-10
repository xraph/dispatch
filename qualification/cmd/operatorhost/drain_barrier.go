package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	drt "github.com/xraph/dispatch/durable/runtime"
)

type fileDrainObserver struct{ before, after string }

func (o fileDrainObserver) BeforeDrain(ctx context.Context, r drt.DrainRequest) error {
	return pauseDrain(ctx, o.before, r)
}
func (o fileDrainObserver) AfterDrain(ctx context.Context, h drt.DrainHandle) error {
	return pauseDrain(ctx, o.after, h)
}
func pauseDrain(ctx context.Context, path string, record any) error {
	if path == "" {
		return nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(file).Encode(record)
	closeErr := file.Close()
	if err := errors.Join(encodeErr, closeErr); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
