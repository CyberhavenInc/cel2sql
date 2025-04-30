package filters

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cockscomb/cel2sql"
	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

const (
	ExistsEquals         = "existsEquals"
	ExistsEqualsCI       = "existsEqualsCI"
	ExistsStarts         = "existsStarts"
	ExistsStartsCI       = "existsStartsCI"
	ExistsEnds           = "existsEnds"
	ExistsEndsCI         = "existsEndsCI"
	ExistsContains       = "existsContains"
	ExistsContainsCI     = "existsContainsCI"
	ExistsRegexp         = "existsRegexp"   // REGEXP_CONTAINS, not anchored.
	ExistsRegexpCI       = "existsRegexpCI" // REGEXP_CONTAINS, not anchored.
	ExistsContainsTextCI = "existsContainsTextCI"
)

var ciFuncs = map[string]struct{}{
	ExistsEqualsCI:   {},
	ExistsStartsCI:   {},
	ExistsEndsCI:     {},
	ExistsContainsCI: {},
	ExistsRegexpCI:   {},
}

var stringListCelType = cel.ListType(cel.StringType)

func makeFunction(name string, ci bool,
	s2s func(target, pattern string) (bool, error),
	s2l func(target string, patterns []string) (bool, error),
	l2s func(targets []string, pattern string) (bool, error),
	l2l func(targets, patterns []string) (bool, error),
) cel.EnvOption {
	var (
		wraps2s = wrapS2S
		wraps2l = wrapS2L
		wrapl2s = wrapL2S
		wrapl2l = wrapL2L
	)
	if ci {
		wraps2s = wrapS2SCI
		wraps2l = wrapS2LCI
		wrapl2s = wrapL2SCI
		wrapl2l = wrapL2LCI
	}
	return cel.Function(name,
		cel.MemberOverload(name+"_string_to_string",
			[]*cel.Type{cel.StringType, cel.StringType}, cel.BoolType,
			cel.BinaryBinding(wraps2s(s2s)),
		),
		cel.MemberOverload(name+"_string_to_list",
			[]*cel.Type{cel.StringType, stringListCelType}, cel.BoolType,
			cel.BinaryBinding(wraps2l(s2l)),
		),
		cel.MemberOverload(name+"_list_to_string",
			[]*cel.Type{stringListCelType, cel.StringType}, cel.BoolType,
			cel.BinaryBinding(wrapl2s(l2s)),
		),
		cel.MemberOverload(name+"_list_to_list",
			[]*cel.Type{stringListCelType, stringListCelType}, cel.BoolType,
			cel.BinaryBinding(wrapl2l(l2l)),
		),
	)
}

var celFunctions = []cel.EnvOption{
	makeFunction(ExistsEquals, false,
		goExistsEqualsStringToString,
		goExistsEqualsStringToList,
		goExistsEqualsListToString,
		goExistsEqualsListToList,
	),
	makeFunction(ExistsEqualsCI, true,
		goExistsEqualsStringToString,
		goExistsEqualsStringToList,
		goExistsEqualsListToString,
		goExistsEqualsListToList,
	),
	makeFunction(ExistsStarts, false,
		goExistsStartsStringToString,
		goExistsStartsStringToList,
		goExistsStartsListToString,
		goExistsStartsListToList,
	),
	makeFunction(ExistsStartsCI, true,
		goExistsStartsStringToString,
		goExistsStartsStringToList,
		goExistsStartsListToString,
		goExistsStartsListToList,
	),
	makeFunction(ExistsEnds, false,
		goExistsEndsStringToString,
		goExistsEndsStringToList,
		goExistsEndsListToString,
		goExistsEndsListToList,
	),
	makeFunction(ExistsEndsCI, true,
		goExistsEndsStringToString,
		goExistsEndsStringToList,
		goExistsEndsListToString,
		goExistsEndsListToList,
	),
	makeFunction(ExistsContains, false,
		goExistsContainsStringToString,
		goExistsContainsStringToList,
		goExistsContainsListToString,
		goExistsContainsListToList,
	),
	makeFunction(ExistsContainsCI, true,
		goExistsContainsStringToString,
		goExistsContainsStringToList,
		goExistsContainsListToString,
		goExistsContainsListToList,
	),
	makeFunction(ExistsRegexp, false,
		goExistsRegexpStringToString,
		goExistsRegexpStringToList,
		goExistsRegexpListToString,
		goExistsRegexpListToList,
	),
	makeFunction(ExistsRegexpCI, true,
		goExistsRegexpStringToString,
		goExistsRegexpStringToList,
		goExistsRegexpListToString,
		goExistsRegexpListToList,
	),
	makeFunction(ExistsContainsTextCI, true,
		goExistsContainsTextStringToString,
		goExistsContainsTextStringToList,
		goExistsContainsTextListToString,
		goExistsContainsTextListToList,
	),
}

var Declarations = cel.EnvOption(func(e *cel.Env) (*cel.Env, error) {
	var err error
	for _, f := range celFunctions {
		e, err = f(e)
		if err != nil {
			return nil, err
		}
	}
	return e, nil
})

type Extension struct {
	maxArgumentsToExpand int
}

type ExtensionOption func(*Extension)

func WithMaxArgumentsToExpand(count int) ExtensionOption {
	return func(ext *Extension) {
		ext.maxArgumentsToExpand = count
	}
}

func NewExtension(opts ...ExtensionOption) *Extension {
	ext := &Extension{
		// Default values.
		maxArgumentsToExpand: 3,
	}
	for _, o := range opts {
		o(ext)
	}
	return ext
}

func (ext *Extension) ImplementsFunction(fun string) bool {
	switch fun {
	case ExistsEquals, ExistsEqualsCI, ExistsStarts, ExistsStartsCI, ExistsEnds, ExistsEndsCI, ExistsContains, ExistsContainsCI, ExistsContainsTextCI, ExistsRegexp, ExistsRegexpCI:
		return true
	}
	return false
}

func (ext *Extension) CallFunction(con *cel2sql.Converter, function string, target *expr.Expr, args []*expr.Expr) error {
	// Optimization: exists*([x]) = exists*(x)
	if cel2sql.IsListType(con.GetType(args[0])) {
		list := args[0].ExprKind.(*expr.Expr_ListExpr).ListExpr
		if len(list.Elements) == 0 {
			con.WriteString("FALSE")
			return nil
		}
		if len(list.Elements) == 1 {
			args = []*expr.Expr{
				list.Elements[0],
			}
		}
	}
	return ext.callFunction(con, function, target, args)
}

func (ext *Extension) callFunction(con *cel2sql.Converter, function string, target *expr.Expr, args []*expr.Expr) error {
	tgtType := con.GetType(target)
	argType := con.GetType(args[0])
	switch function {
	case ExistsEquals, ExistsEqualsCI:
		switch {
		case cel2sql.IsStringType(tgtType):
			if err := writeTarget(con, function, target); err != nil {
				return err
			}
			switch {
			case cel2sql.IsStringType(argType):
				con.WriteString(" = ")
				return writeArg(con, function, args[0], con.Visit)
			case cel2sql.IsListType(argType):
				con.WriteString(" IN UNNEST(")
				if err := con.Visit(args[0]); err != nil {
					return err
				}
				con.WriteString(")")
				return nil
			}
		case cel2sql.IsListType(tgtType):
			switch {
			case cel2sql.IsStringType(argType):
				return ext.callFunction(con, function, args[0], []*expr.Expr{target})
			case cel2sql.IsListType(argType):
				list := args[0].ExprKind.(*expr.Expr_ListExpr).ListExpr
				if 2 <= len(list.Elements) && len(list.Elements) <= ext.maxArgumentsToExpand {
					// Short list of arguments optimization:
					// field.existsEquals(["foo", "bar"]) => "foo" IN field OR "bar" IN field.
					con.WriteString("(")
					for i, elem := range list.Elements {
						con.WriteString("(")
						if err := ext.callFunction(con, function, elem, []*expr.Expr{target}); err != nil {
							return err
						}
						con.WriteString(")")
						if i < len(list.Elements)-1 {
							con.WriteString(" OR ")
						}
					}
					con.WriteString(")")
					return nil
				}
				return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsEqualsCI, startAnchor: true, endAnchor: true, regexEscape: true})
			}
		}
	case ExistsStarts, ExistsStartsCI:
		if cel2sql.IsStringType(tgtType) && cel2sql.IsStringType(argType) {
			if err := writeSimpleCall("STARTS_WITH", con, function, target, args[0]); err != nil {
				return err
			}
			return nil
		}
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsStartsCI, startAnchor: true, regexEscape: true})
	case ExistsEnds, ExistsEndsCI:
		if cel2sql.IsStringType(tgtType) && cel2sql.IsStringType(argType) {
			if err := writeSimpleCall("ENDS_WITH", con, function, target, args[0]); err != nil {
				return err
			}
			return nil
		}
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsEndsCI, endAnchor: true, regexEscape: true})
	case ExistsContains, ExistsContainsCI:
		if cel2sql.IsStringType(tgtType) && cel2sql.IsStringType(argType) {
			if err := writeSimpleCall("0 != INSTR", con, function, target, args[0]); err != nil {
				return err
			}
			return nil
		}
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsContainsCI, regexEscape: true})
	case ExistsContainsTextCI:
		con.WriteString("SEARCH(")
		if err := con.Visit(target); err != nil {
			return err
		}
		con.WriteString(", ")
		if err := con.Visit(args[0]); err != nil {
			return err
		}
		con.WriteString(")")
		return nil
	case ExistsRegexp, ExistsRegexpCI:
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsRegexpCI})
	default:
		return fmt.Errorf("unsupported filter: %v", function)
	}
	return fmt.Errorf("unsupported types: %v.(%v)", tgtType, argType)
}

type regexpOptions struct {
	caseInsensitive bool
	startAnchor     bool
	endAnchor       bool
	regexEscape     bool
}

func writeTarget(con *cel2sql.Converter, function string, target *expr.Expr) error {
	switch con.GetDialect() {
	case cel2sql.SpannerSQL:
		return wrapLower(con, function, target, con.Visit)
	default:
		return wrapCI(con, function, target, con.Visit)
	}
}

// writeArg wraps arg in LOWER function only if flavor is Spanner and
// function is one of Case Insensitive functions. Otherwise, returns next
func writeArg(con *cel2sql.Converter, function string, arg *expr.Expr, next func(expr *expr.Expr) error) error {
	switch con.GetDialect() {
	case cel2sql.SpannerSQL:
		return wrapLower(con, function, arg, next)
	default:
		return next(arg)
	}
}

func wrapCI(con *cel2sql.Converter, function string, arg *expr.Expr, next func(expr *expr.Expr) error) error {
	if _, has := ciFuncs[function]; !has {
		return next(arg)
	}
	con.WriteString("COLLATE(")
	if err := next(arg); err != nil {
		return err
	}
	con.WriteString(", \"und:ci\")")
	return nil
}

func wrapLower(con *cel2sql.Converter, function string, arg *expr.Expr, next func(expr *expr.Expr) error) error {
	if _, has := ciFuncs[function]; !has {
		return next(arg)
	}
	con.WriteString("LOWER(")
	if err := next(arg); err != nil {
		return err
	}
	con.WriteString(")")
	return nil
}

func writeSimpleCall(sqlFunc string, con *cel2sql.Converter, function string, target, arg *expr.Expr) error {
	con.WriteString(sqlFunc + "(")
	if err := writeTarget(con, function, target); err != nil {
		return err
	}
	con.WriteString(", ")
	if err := con.Visit(arg); err != nil {
		return err
	}
	con.WriteString(")")
	return nil
}

// REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(target, "\x00") || "\x00", r"\x00(arg1|arg2|arg3)\x00")
func (ext *Extension) callRegexp(con *cel2sql.Converter, target *expr.Expr, args []*expr.Expr, opts regexpOptions) error {
	tgtType := con.GetType(target)
	useZeroes := cel2sql.IsListType(tgtType)

	con.WriteString("REGEXP_CONTAINS(")
	if useZeroes {
		con.WriteString("\"\\x00\" || ")
	}
	switch {
	case cel2sql.IsStringType(tgtType):
		if err := con.Visit(target); err != nil {
			return err
		}
	case cel2sql.IsListType(tgtType):
		con.WriteString("ARRAY_TO_STRING(")
		if err := con.Visit(target); err != nil {
			return err
		}
		con.WriteString(", \"\\x00\")")
	default:
		return fmt.Errorf("unsupported target type: %v", tgtType)
	}
	if useZeroes {
		con.WriteString(" || \"\\x00\"")
	}
	con.WriteString(", ")
	regexp, err := buildRegex(args[0], opts, useZeroes)
	if err != nil {
		return err
	}
	//replace con.WriteValue with this if params don't work for some reason
	//con.WriteString(fmt.Sprintf("%q", regexp))
	con.WriteValue(regexp)
	con.WriteString(")")
	return nil
}

func buildRegex(expression *expr.Expr, opts regexpOptions, useZeroes bool) (string, error) {
	builder := strings.Builder{}
	if opts.caseInsensitive {
		builder.WriteString("(?i)")
	}
	if opts.startAnchor {
		if useZeroes {
			builder.WriteString("\x00")
		} else {
			builder.WriteString("^")
		}
	}
	builder.WriteString("(")

	arg, err := cel2sql.GetConstValue(expression)
	if err != nil {
		return "", err
	}
	switch value := arg.(type) {
	case string:
		builder.WriteString(joinRegexps([]string{preprocessRegexp(value, useZeroes)}, opts.regexEscape))
	case []interface{}:
		patterns := make([]string, 0, len(value))
		for _, val := range value {
			if pattern, ok := val.(string); ok {
				patterns = append(patterns, preprocessRegexp(pattern, useZeroes))
			} else {
				return "", fmt.Errorf("wrong const value: %v", pattern)
			}
		}
		builder.WriteString(joinRegexps(patterns, opts.regexEscape))
	default:
		return "", fmt.Errorf("wrong const value: %v", value)
	}
	builder.WriteString(")")
	if opts.endAnchor {
		if useZeroes {
			builder.WriteString("\x00")
		} else {
			builder.WriteString("$")
		}
	}
	return builder.String(), nil
}

func preprocessRegexp(pattern string, useZeroes bool) string {
	if !useZeroes {
		return pattern
	}
	if strings.HasPrefix(pattern, "^") {
		pattern = "\x00" + pattern[1:]
	}
	if strings.HasSuffix(pattern, "$") {
		pattern = pattern[:len(pattern)-1] + "\x00"
	}
	return pattern
}

func joinRegexps(patterns []string, escapeItems bool) string {
	if len(patterns) == 1 && !escapeItems {
		return patterns[0]
	}
	parts := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if escapeItems {
			p = regexp.QuoteMeta(p)
		} else {
			p = fmt.Sprintf("(%s)", p)
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, "|")
}
