package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aetherflow/aetherflow/internal/dsl"
	"github.com/aetherflow/aetherflow/internal/store"
	"github.com/aetherflow/aetherflow/internal/workflow"
)

func TestGetInstanceEnforcesTenantIsolation(t *testing.T) {
	repo := &fakeRepo{
		instances: map[string]*store.Instance{
			"tenant-a/inst-1": {
				ID:                "inst-1",
				TenantID:          "tenant-a",
				DefinitionID:      "def-1",
				DefinitionVersion: 1,
				Status:            workflow.InstanceCompleted,
				Input:             map[string]any{},
				State:             workflow.NewRuntimeState(),
			},
		},
		history: map[string][]store.History{
			"tenant-a/inst-1": []store.History{{ID: "hist-1", InstanceID: "inst-1", StepID: "step-1", Status: workflow.StepCompleted}},
		},
	}
	handler := New(repo, nil, nil, dsl.NewValidator(), Options{
		APIKeys: []APIKey{
			{Token: "reader-a", TenantID: "tenant-a", Role: roleReader},
			{Token: "reader-b", TenantID: "tenant-b", Role: roleReader},
		},
	}).Router()

	okReq := httptest.NewRequest(http.MethodGet, "/instances/inst-1", nil)
	okReq.Header.Set("X-API-Key", "reader-a")
	okResp := httptest.NewRecorder()
	handler.ServeHTTP(okResp, okReq)
	if okResp.Code != http.StatusOK {
		t.Fatalf("expected tenant-a reader to fetch instance, got %d: %s", okResp.Code, okResp.Body.String())
	}

	blockedReq := httptest.NewRequest(http.MethodGet, "/instances/inst-1", nil)
	blockedReq.Header.Set("X-API-Key", "reader-b")
	blockedResp := httptest.NewRecorder()
	handler.ServeHTTP(blockedResp, blockedReq)
	if blockedResp.Code != http.StatusNotFound {
		t.Fatalf("expected tenant-b reader to be isolated, got %d: %s", blockedResp.Code, blockedResp.Body.String())
	}
}

func TestReaderCannotCreateDefinition(t *testing.T) {
	handler := New(&fakeRepo{}, nil, nil, dsl.NewValidator(), Options{
		APIKeys: []APIKey{{Token: "reader", TenantID: "tenant-a", Role: roleReader}},
	}).Router()

	req := httptest.NewRequest(http.MethodPost, "/definitions", nil)
	req.Header.Set("X-API-Key", "reader")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected reader to be forbidden, got %d", resp.Code)
	}
}

type fakeRepo struct {
	instances map[string]*store.Instance
	history   map[string][]store.History
}

func (f *fakeRepo) Ping(context.Context) error { return nil }

func (f *fakeRepo) CreateDefinition(context.Context, string, workflow.DefinitionDSL) (*store.Definition, error) {
	return nil, nil
}

func (f *fakeRepo) GetDefinition(context.Context, string) (*store.Definition, error) {
	return nil, nil
}

func (f *fakeRepo) GetDefinitionForTenant(context.Context, string, string) (*store.Definition, error) {
	return nil, nil
}

func (f *fakeRepo) CreateInstance(context.Context, string, *store.Definition, map[string]any) (*store.Instance, error) {
	return nil, nil
}

func (f *fakeRepo) GetInstance(context.Context, string) (*store.Instance, error) {
	return nil, nil
}

func (f *fakeRepo) GetInstanceForTenant(_ context.Context, tenantID string, id string) (*store.Instance, error) {
	instance, ok := f.instances[tenantID+"/"+id]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	return instance, nil
}

func (f *fakeRepo) ClaimInstance(context.Context, string, string, time.Time) (*store.Instance, error) {
	return nil, nil
}

func (f *fakeRepo) ReleaseInstance(context.Context, string, string) error { return nil }

func (f *fakeRepo) ListRecoverableInstances(context.Context, int) ([]store.Instance, error) {
	return nil, nil
}

func (f *fakeRepo) UpdateInstance(context.Context, *store.Instance, int) error { return nil }

func (f *fakeRepo) AppendHistory(context.Context, *store.History) (*store.History, error) {
	return nil, nil
}

func (f *fakeRepo) CompleteHistory(context.Context, string, string, map[string]any, *string) error {
	return nil
}

func (f *fakeRepo) ListHistory(_ context.Context, instanceID string) ([]store.History, error) {
	return f.history["tenant-a/"+instanceID], nil
}

func (f *fakeRepo) ListHistoryForTenant(_ context.Context, tenantID string, instanceID string) ([]store.History, error) {
	return f.history[tenantID+"/"+instanceID], nil
}

func (f *fakeRepo) UpsertTimer(context.Context, store.Timer) error { return nil }

func (f *fakeRepo) DeleteTimer(context.Context, string) error { return nil }

func (f *fakeRepo) FireTimer(context.Context, string) (*store.Timer, bool, error) {
	return nil, false, nil
}

func (f *fakeRepo) ListDueTimers(context.Context, time.Time, int) ([]store.Timer, error) {
	return nil, nil
}

func (f *fakeRepo) ClaimDueTimers(context.Context, time.Time, int) ([]store.Timer, error) {
	return nil, nil
}
