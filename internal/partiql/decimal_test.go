package partiql

import (
	"strings"
	"testing"
)

func mustDec(t *testing.T, s string) Dec {
	t.Helper()
	d, err := ParseDec(s)
	if err != nil {
		t.Fatalf("ParseDec(%q) failed: %v", s, err)
	}
	return d
}

func TestParseDec(t *testing.T) {
	for _, s := range []string{"0", "1", "123", "-7", "1.5", "-0.25", "1E+3", "1.23E+5", "12E-7", "+0.5", ".5"} {
		if _, err := ParseDec(s); err != nil {
			t.Fatalf("ParseDec(%q): %v", s, err)
		}
	}
	for _, s := range []string{"", ".", "abc", "1.2.3", "1e", "1 e3", "--1", "1-"} {
		if _, err := ParseDec(s); err == nil {
			t.Fatalf("ParseDec(%q) should fail", s)
		}
	}
}

func TestDecString(t *testing.T) {
	cases := [][2]string{
		{"1E+3", "1E+3"},
		{"1.23E+5", "1.23E+5"},
		{"1", "1"},
		{"1.0", "1.0"},
		{"0", "0"},
		{"0.0", "0.0"},
		{"-1.5", "-1.5"},
		{"12E-7", "0.0000012"},
		{"0.0000123", "0.0000123"},
		{"0.000000123", "1.23E-7"},
		{"99999999999999999999999999999999999999", "99999999999999999999999999999999999999"},
	}
	for _, c := range cases {
		if got := mustDec(t, c[0]).String(); got != c[1] {
			t.Fatalf("%s rendered %q, want %q", c[0], got, c[1])
		}
	}
}

func TestDecArithmetic(t *testing.T) {
	type c struct{ a, op, b, want string }
	for _, x := range []c{
		{"1", "+", "1", "2"},
		{"1.0", "+", "1.0", "2.0"},
		{"0.1", "+", "0.2", "0.3"},
		{"-1.5", "+", "1.5", "0.0"},
		{"1E+3", "+", "1", "1001"},
		{"2.5", "+", "4", "6.5"},
		{"5", "-", "3", "2"},
		{"2", "-", "5", "-3"},
		{"1.0", "-", "1.0", "0.0"},
		{"2.5", "*", "4", "10.0"},
		{"0.1", "*", "0.2", "0.02"},
	} {
		a, b := mustDec(t, x.a), mustDec(t, x.b)
		var got Dec
		var err error
		switch x.op {
		case "+":
			got, err = decAdd(a, b)
		case "-":
			got, err = decSub(a, b)
		case "*":
			got, err = decMul(a, b)
		}
		if err != nil {
			t.Fatalf("%s %s %s: %v", x.a, x.op, x.b, err)
		}
		if got.String() != x.want {
			t.Fatalf("%s %s %s = %q, want %q", x.a, x.op, x.b, got.String(), x.want)
		}
	}
}

func TestDecDivide(t *testing.T) {
	cases := [][2]string{
		{"1|3", "0.33333333333333333333333333333333333333"},
		{"1|0.5", "2"},
		{"10|4", "2.5"},
		{"6|2", "3"},
		{"0|5", "0"},
		{"0.0|5", "0.0"},
		{"2.4|0.4", "6"},
		{"1|4", "0.25"},
	}
	for _, c := range cases {
		parts := strings.SplitN(c[0], "|", 2)
		d, err := decDiv(mustDec(t, parts[0]), mustDec(t, parts[1]))
		if err != nil {
			t.Fatalf("%s: %v", c[0], err)
		}
		if d.String() != c[1] {
			t.Fatalf("%s = %q, want %q", c[0], d.String(), c[1])
		}
	}
	if _, err := decDiv(mustDec(t, "1"), mustDec(t, "0")); err == nil || err.Error() != "ValidationException: The expression contains a division by zero" {
		t.Fatalf("division by zero: %v", err)
	}
	d, err := decDiv(mustDec(t, "1"), mustDec(t, "7"))
	if err != nil {
		t.Fatal(err)
	}
	if len(d.String()) != 40 {
		t.Fatalf("1/7 = %q (want 0. plus 38 digits)", d.String())
	}
}

func TestDecRangeErrors(t *testing.T) {
	if _, err := decMul(mustDec(t, "1E+64"), mustDec(t, "1E+64")); err == nil || err.Error() != "ValidationException: Number overflow. Resulting number is too large" {
		t.Fatalf("mul overflow: %v", err)
	}
	nines := strings.Repeat("9", 38)
	if _, err := decAdd(mustDec(t, nines), mustDec(t, "0.1")); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("39-digit sum: %v", err)
	}
	if _, err := decDiv(mustDec(t, "1"), mustDec(t, "1E+200")); err == nil || !strings.Contains(err.Error(), "underflow") {
		t.Fatalf("underflow: %v", err)
	}
	if d, err := decAdd(mustDec(t, "1E+38"), mustDec(t, "3E+37")); err != nil || d.String() != "1.3E+38" {
		t.Fatalf("strip normalization: %v %v", d, err)
	}
}

func TestDecCompare(t *testing.T) {
	if mustDec(t, "1.0").Cmp(mustDec(t, "1")) != 0 {
		t.Fatal("1.0 != 1")
	}
	if mustDec(t, "10").Cmp(mustDec(t, "9")) != 1 {
		t.Fatal("10 <= 9")
	}
	if mustDec(t, "-1.5").Cmp(mustDec(t, "-1.4")) != -1 {
		t.Fatal("-1.5 >= -1.4")
	}
	if mustDec(t, "1E+3").Cmp(mustDec(t, "1000")) != 0 {
		t.Fatal("1E+3 != 1000")
	}
}
