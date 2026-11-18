package partiql

import (
	"strings"
	"unicode/utf8"
)

type tokKind int

const (
	tkWord     tokKind = iota // identifier or keyword candidate
	tkNameRef                 // #name
	tkValueRef                // :value
	tkInt                     // digits (list index)
	tkSym                     // . , ( ) [ ] = + - < >
	tkLE                      // <=
	tkGE                      // >=
	tkNE                      // <>
	tkEOF
)

// Token is a lexed token with its byte span in the source expression.
type Token struct {
	Kind  tokKind
	Text  string
	Start int
	End   int
}

// nearRadius is how much preceding source text the ANTLR-style "near"
// context of a grammar failure covers, matching the Java parsers.
const nearRadius = 10

func isDigitByte(b byte) bool { return b >= '0' && b <= '9' }

func isLetterByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isWordByte(b byte) bool { return isLetterByte(b) || isDigitByte(b) }

// foldWord case-folds a word token for context-insensitive keyword matching.
func foldWord(t Token) string { return strings.ToLower(t.Text) }

// symbol lexes one operator / punctuation (possibly two characters). ok is
// false for characters the grammar cannot classify.
func symbol(src string, i int) (k tokKind, text string, width int, ok bool) {
	c := src[i]
	if c == '<' && i+1 < len(src) {
		switch src[i+1] {
		case '>':
			return tkNE, "<>", 2, true
		case '=':
			return tkLE, "<=", 2, true
		}
	}
	if c == '>' && i+1 < len(src) && src[i+1] == '=' {
		return tkGE, ">=", 2, true
	}
	switch c {
	case '.', ',', '(', ')', '[', ']', '=', '+', '-', '<', '>':
		return tkSym, string(c), 1, true
	}
	return tkSym, "", 0, false
}

// lex tokenizes a DDB expression. On failure it returns the offending token
// (the Java lexer reports the first character it cannot classify).
func lex(src string) ([]Token, *Token) {
	toks := make([]Token, 0, 16)
	push := func(k tokKind, text string, s, e int) {
		toks = append(toks, Token{Kind: k, Text: text, Start: s, End: e})
	}
	fail := func(t Token) ([]Token, *Token) {
		toks = append(toks, t)
		return toks, &toks[len(toks)-1]
	}
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#' || c == ':':
			j := i + 1
			for j < len(src) && isWordByte(src[j]) {
				j++
			}
			if j == i+1 {
				return fail(Token{Kind: tkSym, Text: src[i : i+1], Start: i, End: i + 1})
			}
			if c == '#' {
				push(tkNameRef, src[i:j], i, j)
			} else {
				push(tkValueRef, src[i:j], i, j)
			}
			i = j
		case isLetterByte(c):
			j := i + 1
			for j < len(src) && isWordByte(src[j]) {
				j++
			}
			push(tkWord, src[i:j], i, j)
			i = j
		case isDigitByte(c):
			j := i + 1
			for j < len(src) && isDigitByte(src[j]) {
				j++
			}
			push(tkInt, src[i:j], i, j)
			i = j
		default:
			k, text, w, ok := symbol(src, i)
			if !ok {
				_, sz := utf8.DecodeRuneInString(src[i:])
				return fail(Token{Kind: tkSym, Text: src[i : i+sz], Start: i, End: i + sz})
			}
			push(k, text, i, i+w)
			i += w
		}
	}
	push(tkEOF, "<EOF>", len(src), len(src))
	return toks, nil
}

// nearText renders the "near" context of a grammar failure: up to
// nearRadius bytes before the offending token, and the offending token
// itself (nothing for EOF beyond whitespace trimming).
func nearText(src string, t Token) string {
	start := t.Start - nearRadius
	if start < 0 {
		start = 0
	}
	end := t.End
	if end > len(src) {
		end = len(src)
	}
	s := src[start:end]
	if t.Kind == tkEOF {
		return strings.TrimSpace(s)
	}
	return strings.TrimLeft(s, " \t\r\n")
}

func reportText(t Token) string {
	if t.Kind == tkEOF {
		return "<EOF>"
	}
	return t.Text
}
