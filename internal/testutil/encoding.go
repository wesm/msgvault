package testutil

// encodedSamples holds the canonical byte sequences (unexported to prevent direct mutation).
var encodedSamples = struct {
	ShiftJIS_Konnichiwa    []byte
	GBK_Nihao              []byte
	Big5_Nihao             []byte
	EUCKR_Annyeong         []byte
	Win1252_SmartQuoteRight []byte
	Win1252_EnDash          []byte
	Win1252_EmDash          []byte
	Win1252_DoubleQuotes    []byte
	Win1252_Trademark       []byte
	Win1252_Bullet          []byte
	Win1252_Euro            []byte
	Latin1_OAcute           []byte
	Latin1_CCedilla         []byte
	Latin1_UUmlaut          []byte
	Latin1_NTilde           []byte
	Latin1_Registered       []byte
	Latin1_Degree           []byte
}{
	ShiftJIS_Konnichiwa: []byte{0x82, 0xb1, 0x82, 0xf1, 0x82, 0xc9, 0x82, 0xbf, 0x82, 0xcd},
	GBK_Nihao:           []byte{0xc4, 0xe3, 0xba, 0xc3},
	Big5_Nihao:          []byte{0xa9, 0x6f, 0xa6, 0x6e},
	EUCKR_Annyeong:      []byte{0xbe, 0xc8, 0xb3, 0xe7},

	Win1252_SmartQuoteRight: []byte("Rand\x92s Opponent"),
	Win1252_EnDash:          []byte("2020 \x96 2024"),
	Win1252_EmDash:          []byte("Hello\x97World"),
	Win1252_DoubleQuotes:    []byte("\x93Hello\x94"),
	Win1252_Trademark:       []byte("Brand\x99"),
	Win1252_Bullet:          []byte("\x95 Item"),
	Win1252_Euro:            []byte("Price: \x80100"),

	Latin1_OAcute:     []byte("Mir\xf3 - Picasso"),
	Latin1_CCedilla:   []byte("Gar\xe7on"),
	Latin1_UUmlaut:    []byte("M\xfcnchen"),
	Latin1_NTilde:     []byte("Espa\xf1a"),
	Latin1_Registered: []byte("Laguiole.com \xae"),
	Latin1_Degree:     []byte("25\xb0C"),
}

// EncodedSamplesT mirrors the encodedSamples struct type for use by callers.
type EncodedSamplesT struct {
	ShiftJIS_Konnichiwa    []byte
	GBK_Nihao              []byte
	Big5_Nihao             []byte
	EUCKR_Annyeong         []byte
	Win1252_SmartQuoteRight []byte
	Win1252_EnDash          []byte
	Win1252_EmDash          []byte
	Win1252_DoubleQuotes    []byte
	Win1252_Trademark       []byte
	Win1252_Bullet          []byte
	Win1252_Euro            []byte
	Latin1_OAcute           []byte
	Latin1_CCedilla         []byte
	Latin1_UUmlaut          []byte
	Latin1_NTilde           []byte
	Latin1_Registered       []byte
	Latin1_Degree           []byte
}

func cloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}

// EncodedSamples returns a fresh copy of all encoded byte samples, safe for
// mutation by individual tests without cross-test coupling.
func EncodedSamples() EncodedSamplesT {
	return EncodedSamplesT{
		ShiftJIS_Konnichiwa:    cloneBytes(encodedSamples.ShiftJIS_Konnichiwa),
		GBK_Nihao:              cloneBytes(encodedSamples.GBK_Nihao),
		Big5_Nihao:             cloneBytes(encodedSamples.Big5_Nihao),
		EUCKR_Annyeong:         cloneBytes(encodedSamples.EUCKR_Annyeong),
		Win1252_SmartQuoteRight: cloneBytes(encodedSamples.Win1252_SmartQuoteRight),
		Win1252_EnDash:          cloneBytes(encodedSamples.Win1252_EnDash),
		Win1252_EmDash:          cloneBytes(encodedSamples.Win1252_EmDash),
		Win1252_DoubleQuotes:    cloneBytes(encodedSamples.Win1252_DoubleQuotes),
		Win1252_Trademark:       cloneBytes(encodedSamples.Win1252_Trademark),
		Win1252_Bullet:          cloneBytes(encodedSamples.Win1252_Bullet),
		Win1252_Euro:            cloneBytes(encodedSamples.Win1252_Euro),
		Latin1_OAcute:           cloneBytes(encodedSamples.Latin1_OAcute),
		Latin1_CCedilla:         cloneBytes(encodedSamples.Latin1_CCedilla),
		Latin1_UUmlaut:          cloneBytes(encodedSamples.Latin1_UUmlaut),
		Latin1_NTilde:           cloneBytes(encodedSamples.Latin1_NTilde),
		Latin1_Registered:       cloneBytes(encodedSamples.Latin1_Registered),
		Latin1_Degree:           cloneBytes(encodedSamples.Latin1_Degree),
	}
}
