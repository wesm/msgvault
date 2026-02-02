package testutil

// EncodedSamples contains raw byte sequences for common encodings, useful for
// testing charset detection and conversion functions.
var EncodedSamples = struct {
	// ShiftJIS_Konnichiwa is "こんにちは" in Shift-JIS.
	ShiftJIS_Konnichiwa []byte
	// GBK_Nihao is "你好" in GBK (Simplified Chinese).
	GBK_Nihao []byte
	// Big5_Nihao is "你好" in Big5 (Traditional Chinese).
	Big5_Nihao []byte
	// EUCKR_Annyeong is "안녕" in EUC-KR (Korean).
	EUCKR_Annyeong []byte

	// Win1252_SmartQuoteRight is "Rand's Opponent" with a Windows-1252 right single quote (0x92 → U+2019).
	Win1252_SmartQuoteRight []byte
	// Win1252_EnDash is "2020 – 2024" with a Windows-1252 en dash (0x96 → U+2013).
	Win1252_EnDash []byte
	// Win1252_EmDash is "Hello—World" with a Windows-1252 em dash (0x97 → U+2014).
	Win1252_EmDash []byte
	// Win1252_DoubleQuotes is smart double quotes around "Hello" (0x93/0x94 → U+201C/U+201D).
	Win1252_DoubleQuotes []byte
	// Win1252_Trademark is "Brand™" with Windows-1252 trademark (0x99 → U+2122).
	Win1252_Trademark []byte
	// Win1252_Bullet is "• Item" with Windows-1252 bullet (0x95 → U+2022).
	Win1252_Bullet []byte
	// Win1252_Euro is "Price: €100" with Windows-1252 euro sign (0x80 → U+20AC).
	Win1252_Euro []byte

	// Latin1_OAcute is "Miró - Picasso" with Latin-1 ó (0xF3).
	Latin1_OAcute []byte
	// Latin1_CCedilla is "Garçon" with Latin-1 ç (0xE7).
	Latin1_CCedilla []byte
	// Latin1_UUmlaut is "München" with Latin-1 ü (0xFC).
	Latin1_UUmlaut []byte
	// Latin1_NTilde is "España" with Latin-1 ñ (0xF1).
	Latin1_NTilde []byte
	// Latin1_Registered is "Laguiole.com ®" with Latin-1 ® (0xAE).
	Latin1_Registered []byte
	// Latin1_Degree is "25°C" with Latin-1 ° (0xB0).
	Latin1_Degree []byte
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
