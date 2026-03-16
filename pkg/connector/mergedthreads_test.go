package connector

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-gvoice/pkg/libgv/gvproto"
)

func mustGVTestMessage(threadID string, timestamp int64) *gvproto.Message {
	return &gvproto.Message{
		ID:        fmt.Sprintf("%s-%d", threadID, timestamp),
		Timestamp: timestamp,
	}
}

func makeTimestampFetcher(t *testing.T, threads map[string][]*gvproto.Message, calls *[]string) gvMergedBackwardFetcher {
	t.Helper()
	return func(_ context.Context, threadID string, count int, paginationToken string) (*gvproto.Thread, error) {
		*calls = append(*calls, fmt.Sprintf("%s:%d:%s", threadID, count, paginationToken))
		cutoff, err := strconv.ParseInt(paginationToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid pagination token %q: %w", paginationToken, err)
		}
		var page []*gvproto.Message
		for _, msg := range threads[threadID] {
			if msg.Timestamp >= cutoff {
				continue
			}
			page = append(page, msg)
			if len(page) == count {
				break
			}
		}
		nextToken := ""
		if len(page) > 0 {
			lastTimestamp := page[len(page)-1].Timestamp
			for _, msg := range threads[threadID] {
				if msg.Timestamp < lastTimestamp {
					nextToken = strconv.FormatInt(lastTimestamp, 10)
					break
				}
			}
		}
		return &gvproto.Thread{
			ID:              threadID,
			Messages:        page,
			PaginationToken: nextToken,
		}, nil
	}
}

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

func TestFetchMergedBackwardMessagesCapsBatchToCount(t *testing.T) {
	threadIDs := []string{"thread-a", "thread-b"}
	threads := map[string][]*gvproto.Message{
		"thread-a": {
			mustGVTestMessage("thread-a", 105),
			mustGVTestMessage("thread-a", 104),
			mustGVTestMessage("thread-a", 103),
		},
		"thread-b": {
			mustGVTestMessage("thread-b", 102),
			mustGVTestMessage("thread-b", 101),
			mustGVTestMessage("thread-b", 100),
		},
	}
	var calls []string
	messages, nextTokens, err := fetchMergedBackwardMessages(
		context.Background(),
		threadIDs,
		map[string]string{"thread-a": "200", "thread-b": "200"},
		2,
		makeTimestampFetcher(t, threads, &calls),
	)
	if err != nil {
		t.Fatalf("fetchMergedBackwardMessages returned error: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("unexpected message count: got %d want 2", len(messages))
	}
	if messages[0].Timestamp != 104 || messages[1].Timestamp != 105 {
		t.Fatalf("unexpected message timestamps: got [%d %d] want [104 105]", messages[0].Timestamp, messages[1].Timestamp)
	}
	expectedCalls := []string{"thread-a:1:200", "thread-b:1:200", "thread-a:1:105"}
	if len(calls) != len(expectedCalls) {
		t.Fatalf("unexpected fetch call count: got %v want %v", calls, expectedCalls)
	}
	for i, expected := range expectedCalls {
		if calls[i] != expected {
			t.Fatalf("unexpected fetch call at %d: got %q want %q", i, calls[i], expected)
		}
	}
	expectedTokens := map[string]string{
		"thread-a": "104",
		"thread-b": "104",
	}
	if len(nextTokens) != len(expectedTokens) {
		t.Fatalf("unexpected next token count: got %d want %d", len(nextTokens), len(expectedTokens))
	}
	for threadID, token := range expectedTokens {
		if nextTokens[threadID] != token {
			t.Fatalf("unexpected next token for %s: got %q want %q", threadID, nextTokens[threadID], token)
		}
	}
}

func TestFetchMergedBackwardMessagesCursorDoesNotSkipMessages(t *testing.T) {
	threadIDs := []string{"thread-a", "thread-b"}
	threads := map[string][]*gvproto.Message{
		"thread-a": {
			mustGVTestMessage("thread-a", 105),
			mustGVTestMessage("thread-a", 104),
			mustGVTestMessage("thread-a", 103),
		},
		"thread-b": {
			mustGVTestMessage("thread-b", 102),
			mustGVTestMessage("thread-b", 101),
			mustGVTestMessage("thread-b", 100),
		},
	}
	var firstCalls []string
	firstPage, nextTokens, err := fetchMergedBackwardMessages(
		context.Background(),
		threadIDs,
		map[string]string{"thread-a": "200", "thread-b": "200"},
		2,
		makeTimestampFetcher(t, threads, &firstCalls),
	)
	if err != nil {
		t.Fatalf("first fetch returned error: %v", err)
	}
	if len(firstPage) != 2 || firstPage[0].Timestamp != 104 || firstPage[1].Timestamp != 105 {
		t.Fatalf("unexpected first page timestamps: got %v", []int64{firstPage[0].Timestamp, firstPage[1].Timestamp})
	}
	var secondCalls []string
	secondPage, secondNextTokens, err := fetchMergedBackwardMessages(
		context.Background(),
		threadIDs,
		nextTokens,
		2,
		makeTimestampFetcher(t, threads, &secondCalls),
	)
	if err != nil {
		t.Fatalf("second fetch returned error: %v", err)
	}
	if len(secondPage) != 2 {
		t.Fatalf("unexpected second page length: got %d want 2", len(secondPage))
	}
	if secondPage[0].Timestamp != 102 || secondPage[1].Timestamp != 103 {
		t.Fatalf("unexpected second page timestamps: got [%d %d] want [102 103]", secondPage[0].Timestamp, secondPage[1].Timestamp)
	}
	if secondNextTokens["thread-a"] != "" {
		t.Fatalf("thread-a should be exhausted after second page, got token %q", secondNextTokens["thread-a"])
	}
	if secondNextTokens["thread-b"] != "102" {
		t.Fatalf("unexpected next token for thread-b: got %q want %q", secondNextTokens["thread-b"], "102")
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

func TestResolveMergedPortalThreadIDsPrefersTextThreadWhenNoExistingRooms(t *testing.T) {
	resolved := resolveMergedPortalThreadIDs(
		[]string{"call-thread", "sms-thread"},
		"sms-thread",
		nil,
	)
	expected := map[string]string{
		"call-thread": "sms-thread",
		"sms-thread":  "sms-thread",
	}
	if len(resolved) != len(expected) {
		t.Fatalf("unexpected resolution size: got %d want %d", len(resolved), len(expected))
	}
	for threadID, portalThreadID := range expected {
		if resolved[threadID] != portalThreadID {
			t.Fatalf("unexpected canonical thread for %s: got %q want %q", threadID, resolved[threadID], portalThreadID)
		}
	}
}

func TestResolveMergedPortalThreadIDsReusesSingleExistingRoom(t *testing.T) {
	resolved := resolveMergedPortalThreadIDs(
		[]string{"call-thread", "sms-thread"},
		"sms-thread",
		map[string]*database.Portal{
			"call-thread": {
				PortalKey: networkid.PortalKey{ID: "call-thread"},
				MXID:      "!call:example.com",
			},
		},
	)
	expected := map[string]string{
		"call-thread": "call-thread",
		"sms-thread":  "call-thread",
	}
	for threadID, portalThreadID := range expected {
		if resolved[threadID] != portalThreadID {
			t.Fatalf("unexpected canonical thread for %s: got %q want %q", threadID, resolved[threadID], portalThreadID)
		}
	}
}

func TestResolveMergedPortalThreadIDsReusesMergedRoomWhenOnlySecondaryThreadIsKnown(t *testing.T) {
	mergedPortal := &database.Portal{
		PortalKey: networkid.PortalKey{ID: "sms-thread"},
		MXID:      id.RoomID("!sms:example.com"),
		Metadata: &PortalMetadata{
			ThreadIDs: []string{"sms-thread", "call-thread"},
		},
	}
	portalsByThreadID := make(map[string]*database.Portal)
	indexStoredPortalThreadIDs(mergedPortal, portalsByThreadID)

	resolved := resolveMergedPortalThreadIDs(
		[]string{"call-thread"},
		"",
		portalsByThreadID,
	)
	expected := map[string]string{
		"call-thread": "sms-thread",
	}
	for threadID, portalThreadID := range expected {
		if resolved[threadID] != portalThreadID {
			t.Fatalf("unexpected canonical thread for %s: got %q want %q", threadID, resolved[threadID], portalThreadID)
		}
	}
}

func TestResolveMergedPortalThreadIDsMergesMultipleExistingRooms(t *testing.T) {
	resolved := resolveMergedPortalThreadIDs(
		[]string{"call-thread", "voicemail-thread", "sms-thread"},
		"sms-thread",
		map[string]*database.Portal{
			"call-thread": {
				PortalKey: networkid.PortalKey{ID: "call-thread"},
				MXID:      id.RoomID("!call:example.com"),
			},
			"sms-thread": {
				PortalKey: networkid.PortalKey{ID: "sms-thread"},
				MXID:      id.RoomID("!sms:example.com"),
			},
		},
	)
	expected := map[string]string{
		"call-thread":      "sms-thread",
		"voicemail-thread": "sms-thread",
		"sms-thread":       "sms-thread",
	}
	for threadID, portalThreadID := range expected {
		if resolved[threadID] != portalThreadID {
			t.Fatalf("unexpected canonical thread for %s: got %q want %q", threadID, resolved[threadID], portalThreadID)
		}
	}
}
