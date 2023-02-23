package filters

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/require"
)

func TestGoImplementation(t *testing.T) {
	env, err := cel.NewEnv(Declarations)
	require.NoError(t, err)

	cases := []struct {
		expr      string
		want      bool
		wantError bool
	}{

		{
			expr: `"foo".existsEquals("foo")`,
			want: true,
		},
		{
			expr: `"foo".existsEquals("")`,
			want: false,
		},
		{
			expr: `"foo".existsEquals(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `"Foo".existsEquals(["foo", "bar"])`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsEquals("foo")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsEquals("baz")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsEquals(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsEquals(["foo", "bar"])`,
			want: false,
		},
		{
			expr: `"foo".existsEqualsCI("foo")`,
			want: true,
		},
		{
			expr: `"foo".existsEqualsCI("")`,
			want: false,
		},
		{
			expr: `"foo".existsEqualsCI(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `"Foo".existsEqualsCI(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsEqualsCI("foo")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsEqualsCI("baz")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsEqualsCI(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsEqualsCI(["foo", "bar"])`,
			want: true,
		},

		{
			expr: `"foo".existsStarts("foo")`,
			want: true,
		},
		{
			expr: `"foo".existsStarts("fo")`,
			want: true,
		},
		{
			expr: `"foo".existsStarts("")`,
			want: true,
		},
		{
			expr: `"foo".existsStarts(["fo", "bar"])`,
			want: true,
		},
		{
			expr: `"Foo".existsStarts(["fo", "bar"])`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsStarts("fo")`,
			want: true,
		},
		{
			expr: `["foo", "ba"].existsStarts("baz")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsStarts(["fo", "bar"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsStarts(["f", "bar"])`,
			want: false,
		},
		{
			expr: `"foo".existsStartsCI("F")`,
			want: true,
		},
		{
			expr: `"foo".existsStartsCI("")`,
			want: true,
		},
		{
			expr: `"foo".existsStartsCI(["F", "bar"])`,
			want: true,
		},
		{
			expr: `"Foo".existsStartsCI(["f", "bar"])`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsStartsCI("f")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsStartsCI("baz")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsStartsCI(["FOO", "bar"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsStartsCI(["foo", "bar"])`,
			want: true,
		},

		{
			expr: `"foo".existsEnds("foo")`,
			want: true,
		},
		{
			expr: `"foo".existsEnds("oo")`,
			want: true,
		},
		{
			expr: `"foo".existsEnds("o")`,
			want: true,
		},
		{
			expr: `"foo".existsEnds("")`,
			want: true,
		},
		{
			expr: `"foo".existsEnds("f")`,
			want: false,
		},
		{
			expr: `"foo".existsEnds(["oo", "bar"])`,
			want: true,
		},
		{
			expr: `"Foo".existsEnds(["foo", "bar"])`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsEnds("oo")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsEnds("baz")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsEnds(["o", "b"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsEnds(["f", "b"])`,
			want: false,
		},
		{
			expr: `"foo".existsEndsCI("foo")`,
			want: true,
		},
		{
			expr: `"foo".existsEndsCI("O")`,
			want: true,
		},
		{
			expr: `"foo".existsEndsCI("F")`,
			want: false,
		},
		{
			expr: `"foo".existsEndsCI("")`,
			want: true,
		},
		{
			expr: `"foo".existsEndsCI(["o", "bar"])`,
			want: true,
		},
		{
			expr: `"Foo".existsEndsCI(["o", "bar"])`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsEndsCI("O")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsEndsCI("baz")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsEndsCI(["a", "R"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsEndsCI(["O", "R"])`,
			want: true,
		},

		{
			expr: `"foo".existsContains("foo")`,
			want: true,
		},
		{
			expr: `"foo".existsContains("f")`,
			want: true,
		},
		{
			expr: `"foo".existsContains("o")`,
			want: true,
		},
		{
			expr: `"foo".existsContains("oo")`,
			want: true,
		},
		{
			expr: `"foo".existsContains("")`,
			want: true,
		},
		{
			expr: `"foo".existsContains(["o", "bar"])`,
			want: true,
		},
		{
			expr: `"Foo".existsContains(["f", "bar"])`,
			want: false,
		},
		{
			expr: `"Foo".existsContains(["F", "bar"])`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsContains("a")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsContains("fr")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsContains(["o", "r"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsContains(["f", "b"])`,
			want: false,
		},
		{
			expr: `"foo".existsContainsCI("F")`,
			want: true,
		},
		{
			expr: `"foo".existsContainsCI("O")`,
			want: true,
		},
		{
			expr: `"foo".existsContainsCI("fO")`,
			want: true,
		},
		{
			expr: `"FOO".existsContainsCI("oo")`,
			want: true,
		},
		{
			expr: `"FOO".existsContainsCI("ooo")`,
			want: false,
		},
		{
			expr: `"foo".existsContainsCI("")`,
			want: true,
		},
		{
			expr: `"foo".existsContainsCI(["FOO", "bar"])`,
			want: true,
		},
		{
			expr: `"Foo".existsContainsCI(["FOO", "bar"])`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsContainsCI("BAR")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsContainsCI("BAZ")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsContainsCI(["FOO", "BaR"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsContainsCI(["fOO", "bAR"])`,
			want: true,
		},

		{
			expr: `"foo".existsRegexp("f[oa]")`,
			want: true,
		},
		{
			expr: `"foo".existsRegexp("")`,
			want: true,
		},
		{
			expr: `"foo".existsRegexp(["fo+", "b.r"])`,
			want: true,
		},
		{
			expr: `"bar".existsRegexp(["(f)o+", "b.r"])`,
			want: true,
		},
		{
			expr: `"Foo".existsRegexp(["f(o)o", "ba?"])`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsRegexp("f.o")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsRegexp("(ba)r")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsRegexp("baz")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsRegexp([".oo", "ba."])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsRegexp([".oo", "bar"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsRegexp(["[^F]oo", "bar"])`,
			want: false,
		},
		{
			expr: `"foo".existsRegexpCI("fOo")`,
			want: true,
		},
		{
			expr: `"foo".existsRegexpCI("")`,
			want: true,
		},
		{
			expr: `"foo".existsRegexpCI(["foO", "bAr"])`,
			want: true,
		},
		{
			expr: `"Foo".existsRegexpCI(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsRegexpCI("OO")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsRegexpCI("baz")`,
			want: false,
		},
		{
			expr: `["foo", "bar"].existsRegexpCI(["[E-G][N-P]{2}", "baz"])`,
			want: true,
		},
		{
			expr: `["Foo", "Bar"].existsRegexpCI(["foo?", "b?ar"])`,
			want: true,
		},
		{
			expr:      `"foo".existsRegexpCI("f(o")`,
			wantError: true,
		},
		{
			expr:      `["foo", "bar"].existsRegexpCI("f(o")`,
			wantError: true,
		},
		{
			expr:      `"foo".existsRegexpCI(["f(o", "bar"])`,
			wantError: true,
		},
		{
			expr:      `["foo", "bar"].existsRegexpCI(["f(o", "bar"])`,
			wantError: true,
		},

		{
			expr: `"foo".existsContainsTextCI("foo")`,
			want: true,
		},
		{
			expr: `"foo bar".existsContainsTextCI("foo")`,
			want: true,
		},
		{
			expr: `"foo bar".existsContainsTextCI("bar")`,
			want: true,
		},
		{
			expr: `"foo bar".existsContainsTextCI("o b")`,
			want: false,
		},
		{
			expr: `"FOO/123".existsContainsTextCI(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `"BAZ/123".existsContainsTextCI(["foo", "bar"])`,
			want: false,
		},
		{
			expr: `"Foo-1".existsContainsTextCI(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsContainsTextCI("foo")`,
			want: true,
		},
		{
			expr: `["foo", "bar"].existsContainsTextCI("baz")`,
			want: false,
		},
		{
			expr: `["foo.1", "bar.2"].existsContainsTextCI(["foo", "bar"])`,
			want: true,
		},
		{
			expr: `["https://Foo.com", "https://Bar.net"].existsContainsTextCI(["foo", "bar"])`,
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.expr, func(t *testing.T) {
			ast, issues := env.Compile(tc.expr)
			require.Nil(t, issues)
			program, err := env.Program(ast)
			require.NoError(t, err)
			out, _, err := program.Eval(map[string]interface{}{})
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, bool(out.(types.Bool)))
		})
	}

}
