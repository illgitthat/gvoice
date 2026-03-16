package connector

import (
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-gvoice/pkg/libgv/gvproto"
)

func TestPortalSourceThreadIDsFromMetadata(t *testing.T) {
	threadIDs := portalSourceThreadIDsFromMetadata("sms-thread", &PortalMetadata{
		ThreadIDs: []string{"call-thread", "sms-thread", "call-thread"},
	})
	expected := []string{"sms-thread", "call-thread"}
	if !sameThreadIDSet(threadIDs, expected) {
		t.Fatalf("unexpected thread IDs: got %v want %v", threadIDs, expected)
	}

	threadIDs = portalSourceThreadIDsFromMetadata("sms-thread", nil)
	expected = []string{"sms-thread"}
	if !sameThreadIDSet(threadIDs, expected) {
		t.Fatalf("unexpected default thread IDs: got %v want %v", threadIDs, expected)
	}
}

func TestStoredPortalSourceThreadIDs(t *testing.T) {
	portal := &database.Portal{
		PortalKey: networkid.PortalKey{ID: "sms-thread"},
		Metadata: &PortalMetadata{
			ThreadIDs: []string{"call-thread", "sms-thread"},
		},
	}
	expected := []string{"sms-thread", "call-thread"}
	if threadIDs := storedPortalSourceThreadIDs(portal); !sameThreadIDSet(threadIDs, expected) {
		t.Fatalf("unexpected stored thread IDs: got %v want %v", threadIDs, expected)
	}
}

func TestEncodeDecodeGVMergedCursor(t *testing.T) {
	cursor, err := encodeGVMergedCursor(map[string]string{
		"call-thread": "older",
		"sms-thread":  "newer",
	})
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}
	decoded, err := decodeGVMergedCursor(cursor)
	if err != nil {
		t.Fatalf("failed to decode cursor: %v", err)
	}
	expected := map[string]string{
		"call-thread": "older",
		"sms-thread":  "newer",
	}
	if len(decoded) != len(expected) {
		t.Fatalf("unexpected decoded cursor length: got %d want %d", len(decoded), len(expected))
	}
	for threadID, token := range expected {
		if decoded[threadID] != token {
			t.Fatalf("unexpected cursor token for %s: got %q want %q", threadID, decoded[threadID], token)
		}
	}
}

func TestSortAndDedupeGVMessages(t *testing.T) {
	messages := sortAndDedupeGVMessages([]*gvproto.Message{
		{ID: "b", Timestamp: 20},
		{ID: "a", Timestamp: 10},
		{ID: "b", Timestamp: 20},
		{ID: "c", Timestamp: 20},
	})
	expected := []string{"a", "b", "c"}
	if len(messages) != len(expected) {
		t.Fatalf("unexpected message count: got %d want %d", len(messages), len(expected))
	}
	for i, messageID := range expected {
		if messages[i].ID != messageID {
			t.Fatalf("unexpected message order at %d: got %q want %q", i, messages[i].ID, messageID)
		}
	}
}

func TestMergedThreadBackfillNeededOnThreadSetChange(t *testing.T) {
	latestMessage := &database.Message{Timestamp: time.UnixMilli(2000)}
	needsBackfill := mergedThreadBackfillNeeded(
		latestMessage,
		time.UnixMilli(1500),
		[]string{"sms-thread"},
		[]string{"sms-thread", "call-thread"},
	)
	if !needsBackfill {
		t.Fatal("expected thread-set change to force backfill")
	}
}

func TestMergedThreadBackfillNeededFallsBackToTimestamp(t *testing.T) {
	latestMessage := &database.Message{Timestamp: time.UnixMilli(1500)}
	if !mergedThreadBackfillNeeded(latestMessage, time.UnixMilli(2000), []string{"sms-thread"}, []string{"sms-thread"}) {
		t.Fatal("expected newer latest message timestamp to require backfill")
	}
	if mergedThreadBackfillNeeded(latestMessage, time.UnixMilli(1000), []string{"sms-thread"}, []string{"sms-thread"}) {
		t.Fatal("expected unchanged thread set with older timestamp to skip backfill")
	}
}
