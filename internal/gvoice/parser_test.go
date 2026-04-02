package gvoice

import (
	"strings"
	"testing"
	"time"
)

func TestParseVCF(t *testing.T) {
	vcf := `BEGIN:VCARD
VERSION:3.0
FN:
N:;;;;
item1.TEL:+17026083638
item1.X-ABLabel:Google Voice
TEL;TYPE=CELL:+15753222266
END:VCARD
`
	phones, err := parseVCF([]byte(vcf))
	if err != nil {
		t.Fatalf("parseVCF() error: %v", err)
	}
	if phones.GoogleVoice != "+17026083638" {
		t.Errorf("GoogleVoice = %q, want +17026083638", phones.GoogleVoice)
	}
	if phones.Cell != "+15753222266" {
		t.Errorf("Cell = %q, want +15753222266", phones.Cell)
	}
}

func TestParseVCF_MissingGV(t *testing.T) {
	vcf := `BEGIN:VCARD
VERSION:3.0
TEL;TYPE=CELL:+15551234567
END:VCARD
`
	_, err := parseVCF([]byte(vcf))
	if err == nil {
		t.Fatal("expected error for missing GV number")
	}
}

func TestClassifyFile(t *testing.T) {
	tests := []struct {
		filename string
		wantName string
		wantType fileType
		wantErr  bool
	}{
		{
			filename: "Keith Stern - Text - 2020-02-03T17_37_45Z.html",
			wantName: "Keith Stern",
			wantType: fileTypeText,
		},
		{
			filename: "Keith Stern - Received - 2020-02-05T23_26_28Z.html",
			wantName: "Keith Stern",
			wantType: fileTypeReceived,
		},
		{
			filename: "Kicy Motley - Placed - 2020-02-03T20_05_20Z.html",
			wantName: "Kicy Motley",
			wantType: fileTypePlaced,
		},
		{
			filename: "John Doe - Missed - 2020-03-15T10_30_00Z.html",
			wantName: "John Doe",
			wantType: fileTypeMissed,
		},
		{
			filename: "Jane - Voicemail - 2020-04-01T12_00_00Z.html",
			wantName: "Jane",
			wantType: fileTypeVoicemail,
		},
		{
			filename: "Group Conversation - 2020-02-05T17_16_14Z.html",
			wantName: "",
			wantType: fileTypeGroup,
		},
		{
			// Filename without type keyword (some call files lack explicit type)
			filename: "Kicy Motley - 2020-02-03T20_05_20Z.html",
			wantName: "Kicy Motley",
			wantType: fileTypePlaced, // defaults to placed, caller overrides from HTML
		},
		{
			// Timestamp without trailing Z
			filename: "Someone - Text - 2020-01-15T08_30_00.html",
			wantName: "Someone",
			wantType: fileTypeText,
		},
		{
			// Phone number as contact name
			filename: "+12025551234 - Text - 2020-06-01T09_00_00Z.html",
			wantName: "+12025551234",
			wantType: fileTypeText,
		},
		{
			filename: "photo.jpg",
			wantErr:  true,
		},
		{
			filename: "Bills.html",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			name, ft, err := classifyFile(tt.filename)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
			if ft != tt.wantType {
				t.Errorf("type = %v, want %v", ft, tt.wantType)
			}
		})
	}
}

const sampleTextHTML = `<?xml version="1.0" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml"><head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<title>Keith Stern</title></head>
<body><div class="hChatLog hfeed">
<div class="message"><abbr class="dt" title="2020-02-03T11:37:45.632-06:00">Feb 3, 2020</abbr>:
<cite class="sender vcard"><a class="tel" href="tel:+12023065386"><span class="fn">Keith Stern</span></a></cite>:
<q>Cara says you&#39;re coming in tonight? Awesome.</q>
</div> <div class="message"><abbr class="dt" title="2020-02-03T11:59:08.554-06:00">Feb 3, 2020</abbr>:
<cite class="sender vcard"><a class="tel" href="tel:+15753222266"><abbr class="fn" title="">Me</abbr></a></cite>:
<q>I&#39;m looking at a bus getting in 815ish.</q>
</div></div></body></html>`

func TestParseTextHTML(t *testing.T) {
	messages, groupPar, err := parseTextHTML(strings.NewReader(sampleTextHTML))
	if err != nil {
		t.Fatalf("parseTextHTML() error: %v", err)
	}

	if len(groupPar) != 0 {
		t.Errorf("expected no group participants, got %v", groupPar)
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	// First message: from Keith
	m0 := messages[0]
	if m0.SenderPhone != "+12023065386" {
		t.Errorf("m0.SenderPhone = %q, want +12023065386", m0.SenderPhone)
	}
	if m0.SenderName != "Keith Stern" {
		t.Errorf("m0.SenderName = %q, want Keith Stern", m0.SenderName)
	}
	if m0.IsMe {
		t.Error("m0.IsMe should be false")
	}
	if !strings.Contains(m0.Body, "Cara says") {
		t.Errorf("m0.Body = %q, want to contain 'Cara says'", m0.Body)
	}
	// HTML entity should be decoded
	if !strings.Contains(m0.Body, "you're") {
		t.Errorf("m0.Body = %q, expected HTML entities to be decoded", m0.Body)
	}

	// Timestamp
	expectedTime := time.Date(2020, 2, 3, 17, 37, 45, 632000000, time.UTC)
	if !m0.Timestamp.Equal(expectedTime) {
		t.Errorf("m0.Timestamp = %v, want %v", m0.Timestamp, expectedTime)
	}

	// Second message: from Me
	m1 := messages[1]
	if !m1.IsMe {
		t.Error("m1.IsMe should be true")
	}
	if m1.SenderName != "Me" {
		t.Errorf("m1.SenderName = %q, want Me", m1.SenderName)
	}
}

const sampleGroupHTML = `<?xml version="1.0" ?>
<html xmlns="http://www.w3.org/1999/xhtml"><head>
<title>Group Conversation</title></head>
<body><div class="hChatLog hfeed"><div class="participants">Group conversation with:
<cite class="sender vcard"><a class="tel" href="tel:+12022712272"><span class="fn">Cara Morris Stern</span></a></cite>, <cite class="sender vcard"><a class="tel" href="tel:+12023065386"><span class="fn">Keith Stern</span></a></cite></div>
<div class="message"><abbr class="dt" title="2020-02-05T11:16:14.368-06:00">Feb 5, 2020</abbr>:
<cite class="sender vcard"><a class="tel" href="tel:+12022712272"><span class="fn">Cara Morris Stern</span></a></cite>:
<q>Check this out<br></q>
</div> <div class="message"><abbr class="dt" title="2020-02-05T11:17:38.249-06:00">Feb 5, 2020</abbr>:
<cite class="sender vcard"><a class="tel" href="tel:+12023065386"><span class="fn">Keith Stern</span></a></cite>:
<q>Cool<br></q>
</div></div></body></html>`

func TestParseTextHTML_Group(t *testing.T) {
	messages, groupPar, err := parseTextHTML(strings.NewReader(sampleGroupHTML))
	if err != nil {
		t.Fatalf("parseTextHTML() error: %v", err)
	}

	if len(groupPar) != 2 {
		t.Fatalf("expected 2 group participants, got %d", len(groupPar))
	}
	if groupPar[0] != "+12022712272" {
		t.Errorf("groupPar[0] = %q, want +12022712272", groupPar[0])
	}
	if groupPar[1] != "+12023065386" {
		t.Errorf("groupPar[1] = %q, want +12023065386", groupPar[1])
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	// Trailing <br> should be stripped
	if strings.HasSuffix(messages[0].Body, "\n") {
		t.Errorf("body should not end with newline: %q", messages[0].Body)
	}
}

const sampleMMS = `<?xml version="1.0" ?>
<html xmlns="http://www.w3.org/1999/xhtml"><head>
<title>Test</title></head>
<body><div class="hChatLog hfeed">
<div class="message"><abbr class="dt" title="2020-02-05T19:30:44.602-06:00">Feb 5, 2020</abbr>:
<cite class="sender vcard"><a class="tel" href="tel:+12022712272"><span class="fn">Test User</span></a></cite>:
<q></q>
<div><a class="video" href="Group Conversation - 2020-02-05T17_16_14Z-7-1">Video attachment</a></div></div></div></body></html>`

func TestParseTextHTML_MMS(t *testing.T) {
	messages, _, err := parseTextHTML(strings.NewReader(sampleMMS))
	if err != nil {
		t.Fatalf("parseTextHTML() error: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	if len(messages[0].Attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(messages[0].Attachments))
	}

	att := messages[0].Attachments[0]
	if att.MediaType != "video" {
		t.Errorf("attachment MediaType = %q, want video", att.MediaType)
	}
	if att.HrefInHTML != "Group Conversation - 2020-02-05T17_16_14Z-7-1" {
		t.Errorf("attachment HrefInHTML = %q", att.HrefInHTML)
	}
}

const sampleReceivedCallHTML = `<?xml version="1.0" ?>
<html xmlns="http://www.w3.org/1999/xhtml"><head>
<title>Received call from
Keith Stern</title></head>
<body><div class="haudio"><span class="album">Call Log for
</span>
<span class="fn">Received call from
Keith Stern</span>
<div class="contributor vcard">Received call from
<a class="tel" href="tel:+12023065386"><span class="fn">Keith Stern</span></a></div>
<abbr class="published" title="2020-02-05T17:26:28.000-06:00">Feb 5, 2020</abbr>
<br />
<abbr class="duration" title="PT1M23S">(00:01:23)</abbr>
<div class="tags">Labels:
<a rel="tag" href="http://www.google.com/voice#received">Received</a></div>
</div></body></html>`

func TestParseCallHTML_Received(t *testing.T) {
	record, err := parseCallHTML(strings.NewReader(sampleReceivedCallHTML))
	if err != nil {
		t.Fatalf("parseCallHTML() error: %v", err)
	}

	if record.CallType != fileTypeReceived {
		t.Errorf("CallType = %v, want received", record.CallType)
	}
	if record.Phone != "+12023065386" {
		t.Errorf("Phone = %q, want +12023065386", record.Phone)
	}
	if record.Name != "Keith Stern" {
		t.Errorf("Name = %q, want Keith Stern", record.Name)
	}
	if record.Duration != "PT1M23S" {
		t.Errorf("Duration = %q, want PT1M23S", record.Duration)
	}

	expectedTime := time.Date(2020, 2, 5, 23, 26, 28, 0, time.UTC)
	if !record.Timestamp.Equal(expectedTime) {
		t.Errorf("Timestamp = %v, want %v", record.Timestamp, expectedTime)
	}
}

const samplePlacedCallHTML = `<?xml version="1.0" ?>
<html xmlns="http://www.w3.org/1999/xhtml"><head>
<title>Placed call to
Kicy Motley</title></head>
<body><div class="haudio"><span class="album">Call Log for
</span>
<span class="fn">Placed call to
Kicy Motley</span>
<div class="contributor vcard">Placed call to
<a class="tel" href="tel:+17188096446"><span class="fn">Kicy Motley</span></a></div>
<abbr class="published" title="2020-02-03T14:05:20.000-06:00">Feb 3, 2020</abbr>
<br />
<abbr class="duration" title="PT5M8S">(00:05:08)</abbr>
<div class="tags">Labels:
<a rel="tag" href="http://www.google.com/voice#placed">Placed</a></div>
</div></body></html>`

func TestParseCallHTML_Placed(t *testing.T) {
	record, err := parseCallHTML(strings.NewReader(samplePlacedCallHTML))
	if err != nil {
		t.Fatalf("parseCallHTML() error: %v", err)
	}

	if record.CallType != fileTypePlaced {
		t.Errorf("CallType = %v, want placed", record.CallType)
	}
	if record.Phone != "+17188096446" {
		t.Errorf("Phone = %q, want +17188096446", record.Phone)
	}
}

func TestComputeMessageID(t *testing.T) {
	id1 := computeMessageID("+12023065386", "2020-02-03T11:37:45Z", "Hello")
	id2 := computeMessageID("+12023065386", "2020-02-03T11:37:45Z", "Hello")
	id3 := computeMessageID("+12023065386", "2020-02-03T11:37:45Z", "Goodbye")

	if id1 != id2 {
		t.Error("same inputs should produce same ID")
	}
	if id1 == id3 {
		t.Error("different inputs should produce different IDs")
	}
	if len(id1) != 16 {
		t.Errorf("ID length = %d, want 16", len(id1))
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"PT1M23S", "1m 23s"},
		{"PT5M8S", "5m 8s"},
		{"PT0S", "0s"},
		{"PT1H2M3S", "1h 2m 3s"},
		{"PT30S", "30s"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := formatDuration(tt.input)
			if got != tt.want {
				t.Errorf("formatDuration(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestComputeThreadID(t *testing.T) {
	// 1:1 text uses other party's phone
	tid := computeThreadID("+15553334444", fileTypeText, "+12023065386", nil)
	if tid != "+12023065386" {
		t.Errorf("1:1 threadID = %q, want +12023065386", tid)
	}

	// Group uses sorted participants
	tid = computeThreadID("+15553334444", fileTypeGroup, "", []string{"+12023065386", "+12022712272"})
	if tid != "group:+12022712272,+12023065386" {
		t.Errorf("group threadID = %q, want group:+12022712272,+12023065386", tid)
	}

	// Call uses calls: prefix
	tid = computeThreadID("+15553334444", fileTypeReceived, "+12023065386", nil)
	if tid != "calls:+12023065386" {
		t.Errorf("call threadID = %q, want calls:+12023065386", tid)
	}
}

func TestSnippet(t *testing.T) {
	long := strings.Repeat("a", 200)
	s := snippet(long, 100)
	if len(s) != 100 {
		t.Errorf("snippet length = %d, want 100", len(s))
	}

	s = snippet("short", 100)
	if s != "short" {
		t.Errorf("snippet = %q, want short", s)
	}

	// Whitespace normalization
	s = snippet("  hello   world  ", 100)
	if s != "hello world" {
		t.Errorf("snippet = %q, want 'hello world'", s)
	}
}
