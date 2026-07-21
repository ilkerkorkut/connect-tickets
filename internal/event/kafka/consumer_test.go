package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

func TestClaimSummaryCountsPartitions(t *testing.T) {
	claims := map[string][]int32{
		RefreshTopic: {0, 1, 2, 3, 4},
		DLQTopic:     {0},
	}
	summary := claimSummary(claims)
	if summary[RefreshTopic] != 5 {
		t.Errorf("refresh partitions = %d, want 5", summary[RefreshTopic])
	}
	if summary[DLQTopic] != 1 {
		t.Errorf("dlq partitions = %d, want 1", summary[DLQTopic])
	}
}

func TestLogRebalanceEmitsClaimsAndGroup(t *testing.T) {
	buf := &bytes.Buffer{}
	logger, err := logging.NewLogger(config.LogConfig{Level: "info", Format: "json"}, buf)
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	ctx := logging.WithRequestID(context.Background(), "req-rebalance")
	h := groupHandler{logger: logger, groupName: "github-repo-refresh-v1"}
	h.logRebalance(ctx, "kafka session claimed", map[string][]int32{RefreshTopic: {0, 1, 2}})

	out := buf.String()
	if !strings.Contains(out, "kafka session claimed") {
		t.Fatalf("missing message: %s", out)
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &record); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if record["consumer_group"] != "github-repo-refresh-v1" {
		t.Errorf("consumer_group = %v", record["consumer_group"])
	}
	if record["request_id"] != "req-rebalance" {
		t.Errorf("request_id = %v", record["request_id"])
	}
	claims, ok := record["claims"].(map[string]any)
	if !ok {
		t.Fatalf("claims missing: %v", record["claims"])
	}
	if claims[RefreshTopic] != float64(3) {
		t.Errorf("claims[refresh] = %v, want 3", claims[RefreshTopic])
	}
}

func TestLogRebalanceNoopWithoutLogger(t *testing.T) {
	h := groupHandler{groupName: "g"}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	h.logRebalance(context.Background(), "x", map[string][]int32{RefreshTopic: {0}})
}
