// mautrix-gvoice - A Matrix-Google Voice puppeting bridge.
// Copyright (C) 2024 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"encoding/json"
	"fmt"
	"slices"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-gvoice/pkg/libgv/gvproto"
)

type gvMergedThreadBundle struct {
	Threads []*gvproto.Thread
}

type gvMergedThreadCursor struct {
	Threads []gvMergedThreadCursorEntry `json:"threads"`
}

type gvMergedThreadCursorEntry struct {
	ID              string `json:"id"`
	PaginationToken string `json:"pagination_token"`
}

func parseGVBackfillBundle(data any) (*gvMergedThreadBundle, error) {
	switch bundle := data.(type) {
	case nil:
		return nil, nil
	case *gvMergedThreadBundle:
		return bundle, nil
	case *gvproto.Thread:
		return &gvMergedThreadBundle{Threads: []*gvproto.Thread{bundle}}, nil
	default:
		return nil, fmt.Errorf("unexpected bundled backfill data type %T", data)
	}
}

func (bundle *gvMergedThreadBundle) normalizedThreadIDs(canonicalID string) []string {
	if bundle == nil {
		return normalizeSourceThreadIDs(canonicalID, nil)
	}
	threadIDs := make([]string, 0, len(bundle.Threads))
	for _, thread := range bundle.Threads {
		if thread != nil {
			threadIDs = append(threadIDs, thread.ID)
		}
	}
	return normalizeSourceThreadIDs(canonicalID, threadIDs)
}

func (bundle *gvMergedThreadBundle) getThread(threadID string) *gvproto.Thread {
	if bundle == nil {
		return nil
	}
	for _, thread := range bundle.Threads {
		if thread != nil && thread.ID == threadID {
			return thread
		}
	}
	if len(bundle.Threads) == 1 {
		return bundle.Threads[0]
	}
	return nil
}

func normalizeSourceThreadIDs(canonicalID string, threadIDs []string) []string {
	extraIDs := make([]string, 0, len(threadIDs))
	seen := make(map[string]struct{}, len(threadIDs)+1)
	if canonicalID != "" {
		seen[canonicalID] = struct{}{}
	}
	for _, threadID := range threadIDs {
		if threadID == "" {
			continue
		}
		if _, ok := seen[threadID]; ok {
			continue
		}
		seen[threadID] = struct{}{}
		extraIDs = append(extraIDs, threadID)
	}
	slices.Sort(extraIDs)
	if canonicalID == "" {
		return extraIDs
	}
	return append([]string{canonicalID}, extraIDs...)
}

func portalSourceThreadIDs(portal *bridgev2.Portal) []string {
	if portal == nil {
		return nil
	}
	meta, _ := portal.Metadata.(*PortalMetadata)
	if meta == nil || len(meta.ThreadIDs) == 0 {
		return normalizeSourceThreadIDs(string(portal.ID), nil)
	}
	return normalizeSourceThreadIDs(string(portal.ID), meta.ThreadIDs)
}

func backfillThreadIDs(portal *bridgev2.Portal, bundle *gvMergedThreadBundle) []string {
	if bundle != nil && len(bundle.Threads) > 0 {
		return bundle.normalizedThreadIDs(string(portal.ID))
	}
	return portalSourceThreadIDs(portal)
}

func encodeGVMergedCursor(tokens map[string]string) (networkid.PaginationCursor, error) {
	if len(tokens) == 0 {
		return "", nil
	}
	threadIDs := make([]string, 0, len(tokens))
	for threadID, token := range tokens {
		if threadID != "" && token != "" {
			threadIDs = append(threadIDs, threadID)
		}
	}
	if len(threadIDs) == 0 {
		return "", nil
	}
	slices.Sort(threadIDs)
	cursor := gvMergedThreadCursor{
		Threads: make([]gvMergedThreadCursorEntry, 0, len(threadIDs)),
	}
	for _, threadID := range threadIDs {
		cursor.Threads = append(cursor.Threads, gvMergedThreadCursorEntry{
			ID:              threadID,
			PaginationToken: tokens[threadID],
		})
	}
	data, err := json.Marshal(&cursor)
	if err != nil {
		return "", fmt.Errorf("failed to encode merged thread cursor: %w", err)
	}
	return networkid.PaginationCursor(data), nil
}

func decodeGVMergedCursor(cursor networkid.PaginationCursor) (map[string]string, error) {
	if cursor == "" {
		return nil, nil
	}
	var parsed gvMergedThreadCursor
	if err := json.Unmarshal([]byte(cursor), &parsed); err != nil {
		return nil, fmt.Errorf("failed to decode merged thread cursor: %w", err)
	}
	tokens := make(map[string]string, len(parsed.Threads))
	for _, thread := range parsed.Threads {
		if thread.ID != "" && thread.PaginationToken != "" {
			tokens[thread.ID] = thread.PaginationToken
		}
	}
	return tokens, nil
}

func sortAndDedupeGVMessages(messages []*gvproto.Message) []*gvproto.Message {
	seen := make(map[string]struct{}, len(messages))
	filtered := messages[:0]
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		if _, ok := seen[msg.ID]; ok {
			continue
		}
		seen[msg.ID] = struct{}{}
		filtered = append(filtered, msg)
	}
	slices.SortFunc(filtered, func(a, b *gvproto.Message) int {
		switch {
		case a.Timestamp < b.Timestamp:
			return -1
		case a.Timestamp > b.Timestamp:
			return 1
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})
	return filtered
}

func trimForwardGVMessages(messages []*gvproto.Message, anchor *database.Message) ([]*gvproto.Message, bool) {
	if anchor == nil {
		return messages, false
	}
	for i, msg := range messages {
		if networkid.MessageID(msg.ID) == anchor.ID || msg.Timestamp <= anchor.Timestamp.UnixMilli() {
			return messages[:i], true
		}
	}
	return messages, false
}
