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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exmime"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-gvoice/pkg/libgv"
	"go.mau.fi/mautrix-gvoice/pkg/libgv/gvproto"
)

const (
	BackgroundRefreshInterval = 15 * time.Minute
	MinRefreshInterval        = 10 * time.Second
	MinRefreshBurstCount      = 5
	RefreshDelay              = 500 * time.Millisecond
)

var _ bridgev2.BackfillingNetworkAPI = (*GVClient)(nil)

func isNewMessages(evt *libgv.RealtimeEvent) bool {
	w := evt.GetDataWrapper()
	if len(w) == 0 || len(w[0].GetData()) == 0 {
		return false
	}
	d := w[0].GetData()[0].GetEvent().GetSub2().GetData()
	if len(d) == 0 {
		return false
	}
	return bytes.HasPrefix(d[0].GetUnknownBytes(), []byte("["))
}

func (gc *GVClient) handleRealtimeEvent(ctx context.Context, rawEvt any) {
	switch evt := rawEvt.(type) {
	case *libgv.CookieChanged:
		gc.UserLogin.Metadata.(*UserLoginMetadata).Cookies = evt.Cookies
		err := gc.UserLogin.Save(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("Failed to save cookies")
		}
	case *libgv.RealtimeConnected:
		gc.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
		go func() {
			gc.wakeupMessageFetcher <- struct{}{}
			zerolog.Ctx(ctx).Debug().Msg("Woke up message fetcher from connected event")
		}()
	case *libgv.RealtimeEvent:
		if isNewMessages(evt) {
			select {
			case gc.wakeupMessageFetcher <- struct{}{}:
				zerolog.Ctx(ctx).Debug().Msg("Woke up message fetcher from realtime event")
			default:
			}
		}
	}
}

func (gc *GVClient) fetchNewMessagesLoop(ctx context.Context) {
	ctxDone := ctx.Done()
	ticker := time.NewTicker(BackgroundRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-gc.wakeupMessageFetcher:
		case <-ticker.C:
		case <-ctxDone:
			zerolog.Ctx(ctx).Debug().Msg("Stopping message fetcher loop")
			return
		}
		time.Sleep(RefreshDelay)
		err := gc.fetchEventsLimiter.Wait(ctx)
		if err != nil {
			zerolog.Ctx(ctx).Debug().Err(err).Msg("Failed to wait for ratelimiter")
			return
		}
		go gc.fetchNewMessages(ctx)
	}
}

func (gc *GVClient) fetchNewMessages(ctx context.Context) {
	gc.fetchEventsLock.Lock()
	defer gc.fetchEventsLock.Unlock()
	zerolog.Ctx(ctx).Debug().Msg("Fetching new messages")
	resp, err := gc.Client.ListThreads(ctx, gvproto.ThreadFolder_ALL_THREADS, "")
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to list threads")
		return
	}
	// Build map of phone number -> text thread ID for DM threads.
	// This allows merging call/voicemail threads into the same portal as SMS.
	textThreadForPhone := make(map[string]string)
	threadsByPhone := make(map[string][]string)
	for _, thread := range resp.Threads {
		if len(thread.PhoneNumbers) != 1 {
			continue
		}
		phone := thread.PhoneNumbers[0]
		threadsByPhone[phone] = append(threadsByPhone[phone], thread.ID)
		if thread.IsText {
			textThreadForPhone[phone] = thread.ID
		}
	}
	existingPortalsByThreadID := make(map[string]*database.Portal)
	for phone := range threadsByPhone {
		portals, err := gc.Main.Bridge.DB.Portal.GetAllDMsWith(ctx, gc.makeUserID(phone))
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Str("phone", phone).Msg("Failed to load DM portals while resolving merged thread mapping")
			continue
		}
		for _, portal := range portals {
			if portal == nil || (portal.Receiver != "" && portal.Receiver != gc.UserLogin.ID) {
				continue
			}
			indexStoredPortalThreadIDs(portal, existingPortalsByThreadID)
		}
	}
	portalThreadForSource := make(map[string]string, len(resp.Threads))
	for phone, threadIDs := range threadsByPhone {
		resolved := resolveMergedPortalThreadIDs(threadIDs, textThreadForPhone[phone], existingPortalsByThreadID)
		for threadID, portalThreadID := range resolved {
			portalThreadForSource[threadID] = portalThreadID
		}
	}
	type fetchedThread struct {
		Thread         *gvproto.Thread
		PortalThreadID string
		PortalKey      networkid.PortalKey
		PrevMessageTS  time.Time
		Known          bool
	}
	type resyncPortal struct {
		PortalKey       networkid.PortalKey
		ChatInfoThread  *gvproto.Thread
		Threads         []*gvproto.Thread
		LatestMessageTS time.Time
		NeedsResync     bool
	}
	portalResyncs := make(map[string]*resyncPortal)
	fetchedThreads := make([]fetchedThread, 0, len(resp.Threads))
	for _, thread := range resp.Threads {
		if len(thread.Messages) == 0 {
			continue
		}
		lastMessageTS := time.UnixMilli(thread.Messages[0].Timestamp)
		portalThreadID := thread.ID
		if resolvedThreadID, ok := portalThreadForSource[thread.ID]; ok {
			portalThreadID = resolvedThreadID
		}
		portalKey := gc.makePortalKey(portalThreadID)
		portalState, ok := portalResyncs[portalThreadID]
		if !ok {
			portalState = &resyncPortal{PortalKey: portalKey}
			portalResyncs[portalThreadID] = portalState
		}
		if portalState.ChatInfoThread == nil || thread.IsText || thread.ID == portalThreadID {
			portalState.ChatInfoThread = thread
		}
		portalState.Threads = append(portalState.Threads, thread)
		if lastMessageTS.After(portalState.LatestMessageTS) {
			portalState.LatestMessageTS = lastMessageTS
		}
		prevMsg, known := gc.lastEvents[thread.ID]
		gc.lastEvents[thread.ID] = lastMessageTS
		if !known {
			portalState.NeedsResync = true
		}
		fetchedThreads = append(fetchedThreads, fetchedThread{
			Thread:         thread,
			PortalThreadID: portalThreadID,
			PortalKey:      portalKey,
			PrevMessageTS:  prevMsg,
			Known:          known,
		})
	}
	for _, threadState := range fetchedThreads {
		if !threadState.Known || portalResyncs[threadState.PortalThreadID].NeedsResync {
			continue
		}
		for _, msg := range threadState.Thread.Messages {
			ts, txnID, sender := gc.getMessageMeta(msg)
			if !ts.After(threadState.PrevMessageTS) {
				break
			}
			gc.Main.Bridge.QueueRemoteEvent(gc.UserLogin, &simplevent.Message[*gvproto.Message]{
				EventMeta: simplevent.EventMeta{
					Type: bridgev2.RemoteEventMessage,
					LogContext: func(c zerolog.Context) zerolog.Context {
						return c.
							Int64("timestamp", msg.Timestamp).
							Int64("txn_id", msg.TransactionID).
							Str("top_level_sender", msg.GetContact().GetPhoneNumber()).
							Str("mms_sender", msg.GetMMS().GetSenderPhoneNumber())
					},
					PortalKey:   threadState.PortalKey,
					Sender:      sender,
					Timestamp:   ts,
					StreamOrder: msg.Timestamp,
				},
				ConvertMessageFunc: gc.convertMessage,
				Data:               msg,
				ID:                 networkid.MessageID(msg.ID),
				TransactionID:      txnID,
			})
		}
	}
	for _, portalState := range portalResyncs {
		if !portalState.NeedsResync {
			continue
		}
		bundle := &gvMergedThreadBundle{Threads: portalState.Threads}
		previousThreadIDs, err := gc.getStoredPortalThreadIDs(ctx, portalState.PortalKey)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Stringer("portal_key", portalState.PortalKey).Msg("Failed to load existing portal thread IDs before resync")
		}
		sourceThreadIDs := bundle.normalizedThreadIDs(string(portalState.PortalKey.ID))
		latestMessageTS := portalState.LatestMessageTS
		chatInfoThread := portalState.ChatInfoThread
		portalKey := portalState.PortalKey
		gc.Main.Bridge.QueueRemoteEvent(gc.UserLogin, &simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:         bridgev2.RemoteEventChatResync,
				PortalKey:    portalKey,
				CreatePortal: true,
			},
			ChatInfo:        gc.wrapChatInfo(ctx, chatInfoThread, sourceThreadIDs),
			LatestMessageTS: latestMessageTS,
			CheckNeedsBackfillFunc: func(ctx context.Context, latestMessage *database.Message) (bool, error) {
				return mergedThreadBackfillNeeded(latestMessage, latestMessageTS, previousThreadIDs, sourceThreadIDs), nil
			},
			BundledBackfillData: bundle,
		})
	}
}

func (gc *GVClient) getStoredPortalThreadIDs(ctx context.Context, portalKey networkid.PortalKey) ([]string, error) {
	portal, err := gc.Main.Bridge.DB.Portal.GetByIDWithUncertainReceiver(ctx, portalKey)
	if err != nil || portal == nil {
		return nil, err
	}
	return storedPortalSourceThreadIDs(portal), nil
}

func (gc *GVClient) fetchMessagesSingleThread(ctx context.Context, params bridgev2.FetchMessagesParams, threadID string, bundledThread *gvproto.Thread) (*bridgev2.FetchMessagesResponse, error) {
	var thread *gvproto.Thread
	var messagesToConvert []*gvproto.Message
	if bundledThread != nil {
		thread = bundledThread
		messagesToConvert = thread.Messages
	}
	if params.Forward {
		if thread == nil {
			resp, err := gc.Client.GetThread(ctx, threadID, 100, "")
			if err != nil {
				return nil, fmt.Errorf("failed to fetch latest messages: %w", err)
			}
			thread = resp.Thread
			messagesToConvert = thread.Messages
		}
		didCutOff := false
		if params.AnchorMessage != nil {
			for i, msg := range messagesToConvert {
				if networkid.MessageID(msg.ID) == params.AnchorMessage.ID || !time.UnixMilli(msg.Timestamp).After(params.AnchorMessage.Timestamp) {
					messagesToConvert = messagesToConvert[:i]
					didCutOff = true
					break
				}
			}
		}
		for len(messagesToConvert) < params.Count && !didCutOff && thread.PaginationToken != "" {
			resp, err := gc.Client.GetThread(ctx, threadID, 100, thread.PaginationToken)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch messages: %w", err)
			}
			thread = resp.Thread
			messagesToConvert = append(messagesToConvert, thread.Messages...)
			if params.AnchorMessage != nil {
				for i, msg := range messagesToConvert {
					if networkid.MessageID(msg.ID) == params.AnchorMessage.ID || !time.UnixMilli(msg.Timestamp).After(params.AnchorMessage.Timestamp) {
						messagesToConvert = messagesToConvert[:i]
						didCutOff = true
						break
					}
				}
			}
		}
	} else {
		paginationToken := string(params.Cursor)
		if paginationToken == "" {
			if params.AnchorMessage == nil {
				return nil, fmt.Errorf("can't backward backfill without either cursor or anchor message")
			}
			paginationToken = strconv.FormatInt(params.AnchorMessage.Timestamp.UnixMilli(), 10)
		}
		for len(messagesToConvert) < params.Count && paginationToken != "" {
			resp, err := gc.Client.GetThread(ctx, threadID, 100, paginationToken)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch messages: %w", err)
			}
			thread = resp.Thread
			messagesToConvert = append(messagesToConvert, thread.Messages...)
			paginationToken = thread.PaginationToken
		}
		if thread == nil {
			return nil, fmt.Errorf("unexpected state: no thread")
		}
	}
	return &bridgev2.FetchMessagesResponse{
		Messages: gc.convertBackfillMessages(ctx, params.Portal, sortAndDedupeGVMessages(messagesToConvert)),
		Cursor:   networkid.PaginationCursor(thread.PaginationToken),
		HasMore:  thread.PaginationToken != "",
		MarkRead: thread.Read,
	}, nil
}

func (gc *GVClient) fetchMergedMessages(ctx context.Context, params bridgev2.FetchMessagesParams, threadIDs []string, bundle *gvMergedThreadBundle) (*bridgev2.FetchMessagesResponse, error) {
	if params.Forward {
		type threadState struct {
			NextToken string
			Stopped   bool
		}
		states := make(map[string]*threadState, len(threadIDs))
		for _, threadID := range threadIDs {
			states[threadID] = &threadState{}
		}
		allRead := true
		var messagesToConvert []*gvproto.Message
		addThread := func(thread *gvproto.Thread) {
			if thread == nil {
				return
			}
			state, ok := states[thread.ID]
			if !ok {
				state = &threadState{}
				states[thread.ID] = state
			}
			if !thread.Read {
				allRead = false
			}
			filtered, hitAnchor := trimForwardGVMessages(thread.Messages, params.AnchorMessage)
			messagesToConvert = append(messagesToConvert, filtered...)
			state.NextToken = thread.PaginationToken
			state.Stopped = hitAnchor || thread.PaginationToken == ""
		}
		for _, threadID := range threadIDs {
			thread := bundle.getThread(threadID)
			if thread == nil {
				resp, err := gc.Client.GetThread(ctx, threadID, 100, "")
				if err != nil {
					return nil, fmt.Errorf("failed to fetch latest messages for merged thread %s: %w", threadID, err)
				}
				thread = resp.Thread
			}
			addThread(thread)
		}
		for len(messagesToConvert) < params.Count {
			fetchedAny := false
			for _, threadID := range threadIDs {
				state := states[threadID]
				if state == nil || state.Stopped || state.NextToken == "" {
					continue
				}
				resp, err := gc.Client.GetThread(ctx, threadID, 100, state.NextToken)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch merged thread messages for %s: %w", threadID, err)
				}
				addThread(resp.Thread)
				fetchedAny = true
			}
			if !fetchedAny {
				break
			}
		}
		hasMore := false
		for _, state := range states {
			if state != nil && !state.Stopped && state.NextToken != "" {
				hasMore = true
				break
			}
		}
		return &bridgev2.FetchMessagesResponse{
			Messages:                gc.convertBackfillMessages(ctx, params.Portal, sortAndDedupeGVMessages(messagesToConvert)),
			HasMore:                 hasMore,
			MarkRead:                allRead,
			AggressiveDeduplication: true,
		}, nil
	}
	currentTokens, err := decodeGVMergedCursor(params.Cursor)
	if err != nil {
		currentTokens = map[string]string{string(params.Portal.ID): string(params.Cursor)}
		if params.AnchorMessage != nil {
			anchorToken := strconv.FormatInt(params.AnchorMessage.Timestamp.UnixMilli(), 10)
			for _, threadID := range threadIDs {
				if threadID != string(params.Portal.ID) {
					currentTokens[threadID] = anchorToken
				}
			}
		}
	}
	if len(currentTokens) == 0 {
		if params.AnchorMessage == nil {
			return nil, fmt.Errorf("can't backward backfill without either cursor or anchor message")
		}
		anchorToken := strconv.FormatInt(params.AnchorMessage.Timestamp.UnixMilli(), 10)
		currentTokens = make(map[string]string, len(threadIDs))
		for _, threadID := range threadIDs {
			currentTokens[threadID] = anchorToken
		}
	}
	messagesToConvert, currentTokens, err := fetchMergedBackwardMessages(ctx, threadIDs, currentTokens, params.Count, func(ctx context.Context, threadID string, fetchCount int, paginationToken string) (*gvproto.Thread, error) {
		resp, err := gc.Client.GetThread(ctx, threadID, fetchCount, paginationToken)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch merged thread messages for %s: %w", threadID, err)
		}
		return resp.Thread, nil
	})
	if err != nil {
		return nil, err
	}
	cursor, err := encodeGVMergedCursor(currentTokens)
	if err != nil {
		return nil, err
	}
	return &bridgev2.FetchMessagesResponse{
		Messages:                gc.convertBackfillMessages(ctx, params.Portal, messagesToConvert),
		Cursor:                  cursor,
		HasMore:                 len(currentTokens) > 0,
		AggressiveDeduplication: true,
	}, nil
}

func (gc *GVClient) convertBackfillMessages(ctx context.Context, portal *bridgev2.Portal, messagesToConvert []*gvproto.Message) []*bridgev2.BackfillMessage {
	convertedMessages := make([]*bridgev2.BackfillMessage, len(messagesToConvert))
	for i, msg := range messagesToConvert {
		ts, txnID, sender := gc.getMessageMeta(msg)
		converted, _ := gc.convertMessage(ctx, portal, gc.Main.Bridge.Bot, msg)
		convertedMessages[i] = &bridgev2.BackfillMessage{
			ConvertedMessage: converted,
			ID:               networkid.MessageID(msg.ID),
			Sender:           sender,
			TxnID:            txnID,
			Timestamp:        ts,
			StreamOrder:      msg.Timestamp,
		}
	}
	return convertedMessages
}

func (gc *GVClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.Count <= 0 {
		return nil, fmt.Errorf("count must be positive")
	}
	bundle, err := parseGVBackfillBundle(params.BundledData)
	if err != nil {
		return nil, err
	}
	threadIDs := backfillThreadIDs(params.Portal, bundle)
	if len(threadIDs) <= 1 {
		threadID := string(params.Portal.ID)
		if len(threadIDs) == 1 {
			threadID = threadIDs[0]
		}
		return gc.fetchMessagesSingleThread(ctx, params, threadID, bundle.getThread(threadID))
	}
	return gc.fetchMergedMessages(ctx, params, threadIDs, bundle)
}

func (gc *GVClient) getMessageMeta(msg *gvproto.Message) (ts time.Time, txnID networkid.TransactionID, sender bridgev2.EventSender) {
	ts = time.UnixMilli(msg.Timestamp)
	if msg.Type == gvproto.Message_SMS_OUT {
		sender.IsFromMe = true
	} else if senderNum := msg.GetMMS().GetSenderPhoneNumber(); senderNum != "" {
		sender.Sender = gc.makeUserID(senderNum)
	} else {
		sender.Sender = gc.makeUserID(msg.GetContact().GetPhoneNumber())
	}
	if msg.TransactionID != 0 {
		txnID = networkid.TransactionID(strconv.FormatInt(msg.TransactionID, 10))
	}
	return
}

func (gc *GVClient) convertMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *gvproto.Message) (*bridgev2.ConvertedMessage, error) {
	for _, converted := range []*bridgev2.ConvertedMessage{
		convertGVVoicemailMessage(msg),
		convertGVCallMessage(msg),
		convertGVMissedCallMessage(msg),
	} {
		if converted != nil {
			return converted, nil
		}
	}
	if msg.GetText() == "" && msg.GetMMS() == nil {
		logUnknownGVMessage(ctx, msg, "empty converted message body")
		return convertUnknownGVMessage(msg), nil
	}
	converted := gc.convertGVTextOrMMSMessage(ctx, portal, intent, msg)
	if converted == nil || isEmptyGVConvertedMessage(converted) {
		logUnknownGVMessage(ctx, msg, "converted empty text message")
		return convertUnknownGVMessage(msg), nil
	}
	return converted, nil
}

func (gc *GVClient) convertGVTextOrMMSMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *gvproto.Message) *bridgev2.ConvertedMessage {
	var content event.MessageEventContent
	output := &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type:    event.EventMessage,
			Content: &content,
		}},
	}
	content.MsgType = event.MsgText
	if msg.MMS != nil {
		if msg.MMS.Subject != "" {
			content.Body = fmt.Sprintf("**%s**\n%s", msg.MMS.Subject, msg.MMS.Text)
			content.FormattedBody = fmt.Sprintf("<strong>%s</strong><br>%s", msg.MMS.Subject, msg.MMS.Text)
			content.Format = event.FormatHTML
		} else {
			content.Body = msg.MMS.Text
		}
		for i, att := range msg.MMS.Attachments {
			if i == 0 {
				gc.convertMedia(ctx, portal, intent, att, &content)
			} else {
				var anotherPart event.MessageEventContent
				gc.convertMedia(ctx, portal, intent, att, &anotherPart)
				output.Parts = append(output.Parts, &bridgev2.ConvertedMessagePart{
					ID:      networkid.PartID(strconv.Itoa(i)),
					Type:    event.EventMessage,
					Content: &anotherPart,
				})
			}
		}
	} else {
		content.Body = msg.Text
	}
	return output
}

func isEmptyGVConvertedMessage(msg *bridgev2.ConvertedMessage) bool {
	if msg == nil || len(msg.Parts) == 0 || msg.Parts[0] == nil || msg.Parts[0].Content == nil {
		return true
	}
	content := msg.Parts[0].Content
	return content.MsgType == event.MsgText && content.Body == "" && content.FormattedBody == ""
}

func (gc *GVClient) convertMedia(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, att *gvproto.Attachment, into *event.MessageEventContent) {
	if att.Status == gvproto.Attachment_NOT_SUPPORTED {
		addMediaFailure(into, "File type not supported by Google Voice")
		return
	}
	data, mimeType, err := gc.Client.DownloadAttachment(ctx, att.ID)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to download attachment")
		addMediaFailure(into, "Failed to download attachment")
		return
	}
	if att.MimeType != "" {
		mimeType = att.MimeType
	}
	if mimeType == "" {
		mimeType = http.DetectContentType(data)
	}
	origMeta := &gvproto.Attachment_Metadata{}
	for _, meta := range att.Metadata {
		if meta.Size == gvproto.Attachment_Metadata_ORIGINAL {
			origMeta = meta
			break
		} else if origMeta == nil || meta.Width > origMeta.Width {
			origMeta = meta
		}
	}
	prefix := strings.Split(mimeType, "/")[0]
	var msgtype event.MessageType
	switch prefix {
	case "image":
		msgtype = event.MsgImage
	case "audio":
		msgtype = event.MsgAudio
	case "video":
		msgtype = event.MsgVideo
	default:
		prefix = "file"
		msgtype = event.MsgFile
	}
	fileName := prefix + exmime.ExtensionFromMimetype(mimeType)
	into.URL, into.File, err = intent.UploadMedia(ctx, portal.MXID, data, fileName, mimeType)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to reupload attachment")
		addMediaFailure(into, "Failed to reupload attachment")
		return
	}
	into.FileName = fileName
	into.MsgType = msgtype
	into.Info = &event.FileInfo{
		MimeType: mimeType,
		Width:    int(origMeta.Width),
		Height:   int(origMeta.Height),
		Size:     len(data),
	}
}

func addMediaFailure(into *event.MessageEventContent, message string) {
	if into.Body != "" {
		into.Body = fmt.Sprintf("%s\n\n%s", message, into.Body)
		if into.FormattedBody != "" {
			into.FormattedBody = fmt.Sprintf("<p>%s</p><p>%s</p>", message, into.FormattedBody)
		}
	} else {
		into.Body = message
		into.MsgType = event.MsgNotice
	}
}

func convertGVCallMessage(msg *gvproto.Message) *bridgev2.ConvertedMessage {
	if msg.GetText() != "" || msg.GetMMS() != nil {
		return nil
	}
	// Google Voice does not reliably distinguish incoming vs outgoing calls
	// in the API (both appear as INCOMING_CALL), so we label generically.
	switch msg.GetType() {
	case gvproto.Message_INCOMING_CALL, gvproto.Message_INCOMING_CALL_CANCELLED, gvproto.Message_OUTGOING_CALL:
	default:
		return nil
	}
	return newGVCallConvertedMessage("Voice call")
}

func convertGVMissedCallMessage(msg *gvproto.Message) *bridgev2.ConvertedMessage {
	if msg.GetType() != gvproto.Message_MISSED_CALL || msg.GetCoarseType() != gvproto.Message_CALL_TYPE_MISSED {
		return nil
	}
	return newGVCallConvertedMessage("Missed call")
}

func convertGVVoicemailMessage(msg *gvproto.Message) *bridgev2.ConvertedMessage {
	if msg.GetType() != gvproto.Message_VOICEMAIL || msg.GetCoarseType() != gvproto.Message_CALL_TYPE_VOICEMAIL {
		return nil
	}
	body := "Voicemail"
	if transcript := buildGVVoicemailTranscript(msg.GetTranscript()); transcript != "" {
		body = "Voicemail: " + transcript
	}
	return newGVCallConvertedMessage(body)
}

func newGVCallConvertedMessage(body string) *bridgev2.ConvertedMessage {
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    body,
				BeeperActionMessage: &event.BeeperActionMessage{
					Type:     event.BeeperActionMessageCall,
					CallType: event.BeeperActionMessageCallTypeVoice,
				},
			},
		}},
	}
}

func buildGVVoicemailTranscript(transcript *gvproto.Message_Transcript) string {
	if transcript == nil {
		return ""
	}
	var parts []string
	for _, token := range transcript.GetTokens() {
		text := strings.TrimSpace(decodeGVTranscriptToken(token.GetText()))
		if text == "" {
			continue
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, " ")
}

func decodeGVTranscriptToken(text []byte) string {
	if len(text) == 0 || !utf8.Valid(text) {
		return ""
	}
	return string(text)
}

func convertUnknownGVMessage(msg *gvproto.Message) *bridgev2.ConvertedMessage {
	extra := make(map[string]any)
	if data, err := proto.Marshal(msg); err == nil {
		encodedMsg := base64.StdEncoding.EncodeToString(data)
		if len(encodedMsg) < 16*1024 {
			extra["fi.mau.gvoice.unsupported_message_data"] = encodedMsg
		}
	}
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgNotice,
				Body:    "Unknown message type, please view it on the Google Voice app",
			},
			Extra: extra,
		}},
	}
}

func logUnknownGVMessage(ctx context.Context, msg *gvproto.Message, reason string) {
	zerolog.Ctx(ctx).Warn().
		Str("reason", reason).
		Str("message_id", msg.GetID()).
		Str("message_type", msg.GetType().String()).
		Str("coarse_type", msg.GetCoarseType().String()).
		Str("text", msg.GetText()).
		Bool("has_mms", msg.GetMMS() != nil).
		Msg("Encountered unknown Google Voice message")
}
