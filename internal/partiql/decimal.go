package partiql

import (
	"math/big"
	"strconv"
	"strings"
)

// maxPrecision mirrors the 38 significant digits of precision supported by
// DynamoDB numbers; the Java evaluator drives BigDecimal with MathContext 38.
const maxPrecision = 38

// Adjusted exponent bounds of the Java/DynamoDB decimal range.
const (
	maxExponent = 126
	minExponent = -128
)

var (
	bigOne   = big.NewInt(1)
	bigFive  = big.NewInt(5)
	bigTen   = big.NewInt(10)
)

// Dec is an exact base-10 decimal equivalent of java.math.BigDecimal:
// value = unscaled * 10^-scale. Rendering follows BigDecimal.toString().
type Dec struct {
	U *big.Int
	S int
}

func pow10(n int) *big.Int {
	if n <= 0 {
		return big.NewInt(1)
	}
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

func lenAbsDigits(u *big.Int) int {
	if u.Sign() == 0 {
		return 1
	}
	return len(new(big.Int).Abs(u).String())
}

// ParseDec parses the wire format of an "N" attribute value, accepting the
// BigDecimal constructor syntax: [+-]digits[.digits][eE[+-]digits].
func ParseDec(s string) (Dec, error) {
	i := 0
	neg := false
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	d0 := i
	for i < len(s) && isDigitByte(s[i]) {
		i++
	}
	intDigits := i - d0
	fracDigits := 0
	if i < len(s) && s[i] == '.' {
		fs := i + 1
		i++
		for i < len(s) && isDigitByte(s[i]) {
			i++
		}
		fracDigits = i - fs
	}
	if intDigits == 0 && fracDigits == 0 {
		return Dec{}, badValueErr("Invalid decimal value: %q", s)
	}
	mant := strings.Replace(s[d0:i], ".", "", 1)
	scale := fracDigits
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		esign := 1
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			if s[i] == '-' {
				esign = -1
			}
			i++
		}
		es := i
		for i < len(s) && isDigitByte(s[i]) {
			i++
		}
		if i == es {
			return Dec{}, badValueErr("Invalid decimal value: %q", s)
		}
		expv := 0
		for _, ch := range s[es:i] {
			expv = expv*10 + int(ch-'0')
			if expv > 1000000 {
				expv = 1000000
			}
		}
		scale -= esign * expv
	}
	if i != len(s) {
		return Dec{}, badValueErr("Invalid decimal value: %q", s)
	}
	u := new(big.Int)
	if _, ok := u.SetString(mant, 10); !ok {
		return Dec{}, badValueErr("Invalid decimal value: %q", s)
	}
	if neg {
		u.Neg(u)
	}
	return Dec{U: u, S: scale}.fixed()
}

// fixed re-normalizes a decimal against the DynamoDB precision and exponent
// range exactly like the Java number post-processing: trailing decimal zeros
// may be shed to stay within 38 digits of precision, otherwise the operation
// overflows.
func (d Dec) fixed() (Dec, error) {
	u := new(big.Int).Set(d.U)
	s := d.S
	if u.Sign() == 0 {
		if s > 128 {
			s = 128
		}
		if s < -128 {
			s = -128
		}
		return Dec{U: u, S: s}, nil
	}
	for lenAbsDigits(u) > maxPrecision {
		rem := new(big.Int).Mod(u, bigTen)
		if rem.Sign() != 0 {
			break
		}
		u.Quo(u, bigTen)
		s--
	}
	if lenAbsDigits(u) > maxPrecision {
		return Dec{}, numberOverflowErr()
	}
	adj := lenAbsDigits(u) - 1 - s
	if adj > maxExponent {
		return Dec{}, numberOverflowErr()
	}
	if adj < minExponent {
		return Dec{}, numberUnderflowErr()
	}
	return Dec{U: u, S: s}, nil
}

// String renders the decimal exactly like java.math.BigDecimal.toString()
// (plain notation for scale >= 0 and adjusted exponent >= -6, scientific
// notation otherwise).
func (d Dec) String() string {
	u := d.U
	neg := u.Sign() < 0
	if u.Sign() == 0 {
		if d.S <= 0 {
			return "0"
		}
		return "0." + strings.Repeat("0", d.S)
	}
	digits := new(big.Int).Abs(u).String()
	p := len(digits)
	s := d.S
	adj := p - 1 - s
	useSci := s < 0 || adj < -6
	if !useSci {
		var out string
		switch {
		case s == 0:
			out = digits
		case p > s:
			out = digits[:p-s] + "." + digits[p-s:]
		default:
			out = "0." + strings.Repeat("0", s-p) + digits
		}
		if neg {
			out = "-" + out
		}
		return out
	}
	coeff := digits[:1]
	if p > 1 {
		coeff += "." + digits[1:]
	}
	signCh := "+"
	if adj < 0 {
		signCh = "-"
		adj = -adj
	}
	out := coeff + "E" + signCh + strconv.Itoa(adj)
	if neg {
		out = "-" + out
	}
	return out
}

// Cmp compares two decimals numerically (scale-insensitive, like
// BigDecimal.compareTo).
func (d Dec) Cmp(o Dec) int {
	hi := d.S
	if o.S > hi {
		hi = o.S
	}
	ua := new(big.Int).Mul(d.U, pow10(hi-d.S))
	ub := new(big.Int).Mul(o.U, pow10(hi-o.S))
	return ua.Cmp(ub)
}

func decAdd(a, b Dec) (Dec, error) { return decAddOp(a, b, false) }

func decSub(a, b Dec) (Dec, error) { return decAddOp(a, b, true) }

// decAddOp implements BigDecimal add/subtract: scale of the wider operand.
func decAddOp(a, b Dec, sub bool) (Dec, error) {
	hi := a.S
	if b.S > hi {
		hi = b.S
	}
	ua := new(big.Int).Mul(a.U, pow10(hi-a.S))
	ub := new(big.Int).Mul(b.U, pow10(hi-b.S))
	if sub {
		ub.Neg(ub)
	}
	ua.Add(ua, ub)
	return Dec{U: ua, S: hi}.fixed()
}

// decMul implements BigDecimal multiply: scale of the sum of scales.
func decMul(a, b Dec) (Dec, error) {
	u := new(big.Int).Mul(a.U, b.U)
	return Dec{U: u, S: a.S + b.S}.fixed()
}

// decDiv implements the Java evaluator's division: exact when the quotient
// terminates, otherwise rounded to 38 significant digits half-even (as with
// MathContext(38)).
func decDiv(a, b Dec) (Dec, error) {
	if b.U.Sign() == 0 {
		return Dec{}, divisionByZeroErr()
	}
	if a.U.Sign() == 0 {
		s := a.S - b.S
		if s > 128 {
			s = 128
		}
		if s < -128 {
			s = -128
		}
		return Dec{U: big.NewInt(0), S: s}, nil
	}
	num := new(big.Int).Abs(a.U)
	den := new(big.Int).Abs(b.U)
	// ratio = (|a| / 10^a.S) / (|b| / 10^b.S) = (|a| * 10^b.S) / (|b| * 10^a.S)
	if a.S > 0 {
		den.Mul(den, pow10(a.S))
	} else if a.S < 0 {
		num.Mul(num, pow10(-a.S))
	}
	if b.S > 0 {
		num.Mul(num, pow10(b.S))
	} else if b.S < 0 {
		den.Mul(den, pow10(-b.S))
	}
	if g := new(big.Int).GCD(nil, nil, num, den); g.Sign() > 0 {
		num.Quo(num, g)
		den.Quo(den, g)
	}
	neg := (a.U.Sign() < 0) != (b.U.Sign() < 0)
	// exactness: denominator must be 2^c2 * 5^c5.
	d := new(big.Int).Set(den)
	c2, c5 := 0, 0
	for d.Sign() > 0 && d.Bit(0) == 0 {
		d.Rsh(d, 1)
		c2++
	}
	rem := new(big.Int)
	for {
		rem.Mod(d, bigFive)
		if rem.Sign() != 0 {
			break
		}
		d.Quo(d, bigFive)
		c5++
	}
	var u *big.Int
	var s int
	if d.Cmp(bigOne) == 0 {
		k := c2
		if c5 > k {
			k = c5
		}
		u = new(big.Int).Mul(num, pow10(k))
		u.Quo(u, den)
		s = k
	} else {
		// round to maxPrecision significant digits, half-even
		cmpPow := func(e int) int { // compares |q| against 10^e
			if e >= 0 {
				return num.Cmp(new(big.Int).Mul(den, pow10(e)))
			}
			return new(big.Int).Mul(num, pow10(-e)).Cmp(den)
		}
		e := len(num.String()) - len(den.String())
		for cmpPow(e) < 0 {
			e--
		}
		for cmpPow(e+1) >= 0 {
			e++
		}
		shift := maxPrecision - 1 - e
		if shift >= 0 {
			n2 := new(big.Int).Mul(num, pow10(shift))
			q, r := new(big.Int).QuoRem(n2, den, new(big.Int))
			u = roundHalfEven(q, r, den)
			s = shift
		} else {
			d2 := new(big.Int).Mul(den, pow10(-shift))
			q, r := new(big.Int).QuoRem(num, d2, new(big.Int))
			u = roundHalfEven(q, r, d2)
			s = shift
		}
		if u.Cmp(pow10(maxPrecision)) >= 0 {
			u.Quo(u, bigTen)
			e++
			s = maxPrecision - 1 - e
		}
	}
	if u.Sign() > 0 && neg {
		u.Neg(u)
	}
	return Dec{U: u, S: s}.fixed()
}

func roundHalfEven(q, r, den *big.Int) *big.Int {
	c := new(big.Int).Lsh(r, 1).Cmp(den)
	switch {
	case c > 0:
		q.Add(q, bigOne)
	case c == 0:
		if q.Bit(0) == 1 {
			q.Add(q, bigOne)
		}
	}
	return q
}
