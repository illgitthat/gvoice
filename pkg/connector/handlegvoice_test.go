package connector

import (
	"testing"

	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-gvoice/pkg/libgv/gvproto"
)

func TestConvertGVVoicemailMessageUsesCallAction(t *testing.T) {
	converted := convertGVVoicemailMessage(&gvproto.Message{
		Type:       gvproto.Message_VOICEMAIL,
		CoarseType: gvproto.Message_CALL_TYPE_VOICEMAIL,
		Transcript: &gvproto.Message_Transcript{
			Tokens: []*gvproto.Message_TranscriptToken{{Text: []byte("please call me back")}},
		},
	})
	if converted == nil {
		t.Fatal("expected voicemail message to convert")
	}
	if len(converted.Parts) != 1 {
		t.Fatalf("unexpected part count: got %d want 1", len(converted.Parts))
	}
	content := converted.Parts[0].Content
	if content == nil {
		t.Fatal("expected voicemail content")
	}
	if content.Body != "Voicemail: please call me back" {
		t.Fatalf("unexpected voicemail body: got %q", content.Body)
	}
	if content.MsgType != event.MsgText {
		t.Fatalf("unexpected voicemail msgtype: got %q want %q", content.MsgType, event.MsgText)
	}
	if content.BeeperActionMessage == nil {
		t.Fatal("expected voicemail call action metadata")
	}
	if content.BeeperActionMessage.Type != event.BeeperActionMessageCall {
		t.Fatalf("unexpected action type: got %q want %q", content.BeeperActionMessage.Type, event.BeeperActionMessageCall)
	}
	if content.BeeperActionMessage.CallType != event.BeeperActionMessageCallTypeVoice {
		t.Fatalf("unexpected call type: got %q want %q", content.BeeperActionMessage.CallType, event.BeeperActionMessageCallTypeVoice)
	}
}

func TestConvertGVVoicemailMessageWithoutTranscript(t *testing.T) {
	converted := convertGVVoicemailMessage(&gvproto.Message{
		Type:       gvproto.Message_VOICEMAIL,
		CoarseType: gvproto.Message_CALL_TYPE_VOICEMAIL,
	})
	if converted == nil {
		t.Fatal("expected voicemail message to convert")
	}
	content := converted.Parts[0].Content
	if content == nil {
		t.Fatal("expected voicemail content")
	}
	if content.Body != "Voicemail" {
		t.Fatalf("unexpected voicemail body without transcript: got %q", content.Body)
	}
	if content.BeeperActionMessage == nil {
		t.Fatal("expected voicemail call action metadata")
	}
}
