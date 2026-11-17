package partiql

import (
  "fmt"
  "strings"
)

type tokKind int

const (
  tokEOF tokKind = iota
  tokIdent
  tokKeyword
  tokString
  tokNumber
  tokParam
  tokSymbol
)

type token struct {
  Kind tokKind
  Text string
  Pos  int
}

func (t token) isKeyword(word string) bool { return t.Kind == tokKeyword && t.Text == word }
func (t token) isSymbol(s string) bool     { return t.Kind == tokSymbol && t.Text == s }

// keywords terminate identifier scanning; quoted identifiers keep their text.
var keywords = map[string]bool{
  "SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true,
  "NOT": true, "IN": true, "BETWEEN": true, "IS": true, "NULL": true,
  "MISSING": true, "TRUE": true, "FALSE": true,
  "INSERT": true, "INTO": true, "VALUE": true, "VALUES": true,
  "UPDATE": true, "SET": true, "REMOVE": true, "DELETE": true,
  "CREATE": true, "TABLE": true, "DROP": true, "PRIMARY": true, "KEY": true,
  "HASH": true, "RANGE": true,
  "RETURNING": true, "ALL": true, "OLD": true, "NEW": true,
  "MODIFIED": true, "ITEMS": true, "AS": true, "DISTINCT": true,
  "ASC": true, "DESC": true, "LIMIT": true,
}

var twoChar = map[string]bool{"<=": true, ">=": true, "<>": true, "!=": true, "||": true}

func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isIdentPart(c byte) bool  { return isIdentStart(c) || isDigit(c) }

type lexer struct {
  src  string
  pos  int
  toks []token
}

func tokenize(src string) ([]token, error) {
  l := &lexer{src: src}
  if err := l.run(); err != nil {
    return nil, err
  }
  return l.toks, nil
}

func (l *lexer) run() error {
  for l.pos < len(l.src) {
    c := l.src[l.pos]
    switch {
    case c == ' ' || c == '\t' || c == '\n' || c == '\r':
      l.pos++
    case c == '"':
      if err := l.quotedIdent(); err != nil {
        return err
      }
    case c == '\'':
      if err := l.stringLit(); err != nil {
        return err
      }
    case isDigit(c) || (c == '.' && l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1])):
      l.number()
    case isIdentStart(c):
      start := l.pos
      for l.pos < len(l.src) && isIdentPart(l.src[l.pos]) {
        l.pos++
      }
      word := l.src[start:l.pos]
      if up := strings.ToUpper(word); keywords[up] {
        l.toks = append(l.toks, token{Kind: tokKeyword, Text: up, Pos: start})
      } else {
        l.toks = append(l.toks, token{Kind: tokIdent, Text: word, Pos: start})
      }
    case c == '?':
      start := l.pos
      l.pos++
      j := l.pos
      for j < len(l.src) && isDigit(l.src[j]) {
        j++
      }
      if j > l.pos {
        l.toks = append(l.toks, token{Kind: tokParam, Text: l.src[start:j], Pos: start})
        l.pos = j
      } else {
        l.toks = append(l.toks, token{Kind: tokParam, Text: "?", Pos: start})
      }
    case c == ':':
      start := l.pos
      l.pos++
      if l.pos < len(l.src) && isIdentStart(l.src[l.pos]) {
        j := l.pos
        for j < len(l.src) && isIdentPart(l.src[j]) {
          j++
        }
        l.toks = append(l.toks, token{Kind: tokParam, Text: l.src[start:j], Pos: start})
        l.pos = j
      } else {
        l.toks = append(l.toks, token{Kind: tokSymbol, Text: ":", Pos: start})
      }
    default:
      l.symbol()
    }
  }
  l.toks = append(l.toks, token{Kind: tokEOF})
  return nil
}

func (l *lexer) symbol() {
  start := l.pos
  l.pos++
  if l.pos < len(l.src) && twoChar[l.src[start:l.pos+1]] {
    l.pos++
  }
  l.toks = append(l.toks, token{Kind: tokSymbol, Text: l.src[start:l.pos], Pos: start})
}

func (l *lexer) number() {
  start := l.pos
  for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
    l.pos++
  }
  if l.pos+1 < len(l.src) && l.src[l.pos] == '.' && isDigit(l.src[l.pos+1]) {
    l.pos++
    for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
      l.pos++
    }
  }
  if l.pos < len(l.src) && (l.src[l.pos] == 'e' || l.src[l.pos] == 'E') {
    save := l.pos
    l.pos++
    if l.pos < len(l.src) && (l.src[l.pos] == '+' || l.src[l.pos] == '-') {
      l.pos++
    }
    j := l.pos
    for j < len(l.src) && isDigit(l.src[j]) {
      j++
    }
    if j > l.pos {
      l.pos = j
    } else {
      l.pos = save
    }
  }
  l.toks = append(l.toks, token{Kind: tokNumber, Text: l.src[start:l.pos], Pos: start})
}

func (l *lexer) quotedIdent() error {
  start := l.pos
  l.pos++
  var sb strings.Builder
  for l.pos < len(l.src) {
    c := l.src[l.pos]
    if c == '"' {
      if l.pos+1 < len(l.src) && l.src[l.pos+1] == '"' {
        sb.WriteByte('"')
        l.pos += 2
        continue
      }
      l.pos++
      l.toks = append(l.toks, token{Kind: tokIdent, Text: sb.String(), Pos: start})
      return nil
    }
    sb.WriteByte(c)
    l.pos++
  }
  return l.fail(start, "unterminated quoted identifier")
}

func (l *lexer) stringLit() error {
  start := l.pos
  l.pos++
  var sb strings.Builder
  for l.pos < len(l.src) {
    c := l.src[l.pos]
    if c == '\'' {
      if l.pos+1 < len(l.src) && l.src[l.pos+1] == '\'' {
        sb.WriteByte('\'')
        l.pos += 2
        continue
      }
      l.pos++
      l.toks = append(l.toks, token{Kind: tokString, Text: sb.String(), Pos: start})
      return nil
    }
    sb.WriteByte(c)
    l.pos++
  }
  return l.fail(start, "unterminated string literal")
}

func (l *lexer) fail(pos int, msg string) error {
  return &Error{Code: CodeValidation, Msg: fmt.Sprintf("syntax error at offset %d: %s", pos, msg)}
}
