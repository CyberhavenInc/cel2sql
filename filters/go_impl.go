package filters

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter/functions"
)

var (
	stringType     = reflect.TypeOf((*string)(nil)).Elem()
	stringListType = reflect.TypeOf((*[]string)(nil)).Elem()
)

func asString(arg ref.Val) (value string, err ref.Val) {
	value0, err0 := arg.ConvertToNative(stringType)
	if err0 != nil {
		return "", types.NewErr("failed to convert to string: %w", err0)
	}
	return value0.(string), nil
}

func asStringList(arg ref.Val) (value []string, err ref.Val) {
	value0, err0 := arg.ConvertToNative(stringListType)
	if err0 != nil {
		return nil, types.NewErr("failed to convert to []string: %w", err0)
	}
	return value0.([]string), nil
}

func lowerList(input []string) []string {
	output := make([]string, 0, len(input))
	for _, str := range input {
		output = append(output, strings.ToLower(str))
	}
	return output
}

func boolOrErr(ok bool, err error) ref.Val {
	if err != nil {
		return types.NewErr("wrapped function failed: %w", err)
	}
	return types.Bool(ok)
}

func wrapS2S(f func(target, pattern string) (bool, error)) functions.BinaryOp {
	return func(target0, pattern0 ref.Val) ref.Val {
		target, err := asString(target0)
		if err != nil {
			return err
		}
		pattern, err := asString(pattern0)
		if err != nil {
			return err
		}
		return boolOrErr(f(target, pattern))
	}
}

func wrapS2L(f func(target string, patterns []string) (bool, error)) functions.BinaryOp {
	return func(target0, patterns0 ref.Val) ref.Val {
		target, err := asString(target0)
		if err != nil {
			return err
		}
		patterns, err := asStringList(patterns0)
		if err != nil {
			return err
		}
		return boolOrErr(f(target, patterns))
	}
}

func wrapL2S(f func(targets []string, pattern string) (bool, error)) functions.BinaryOp {
	return func(targets0, pattern0 ref.Val) ref.Val {
		targets, err := asStringList(targets0)
		if err != nil {
			return err
		}
		pattern, err := asString(pattern0)
		if err != nil {
			return err
		}
		return boolOrErr(f(targets, pattern))
	}
}

func wrapL2L(f func(targets, patterns []string) (bool, error)) functions.BinaryOp {
	return func(targets0, patterns0 ref.Val) ref.Val {
		targets, err := asStringList(targets0)
		if err != nil {
			return err
		}
		patterns, err := asStringList(patterns0)
		if err != nil {
			return err
		}
		return boolOrErr(f(targets, patterns))
	}
}

func wrapS2SCI(f func(target, pattern string) (bool, error)) functions.BinaryOp {
	return func(target0, pattern0 ref.Val) ref.Val {
		target, err := asString(target0)
		if err != nil {
			return err
		}
		pattern, err := asString(pattern0)
		if err != nil {
			return err
		}
		return boolOrErr(f(strings.ToLower(target), strings.ToLower(pattern)))
	}
}

func wrapS2LCI(f func(target string, patterns []string) (bool, error)) functions.BinaryOp {
	return func(target0, patterns0 ref.Val) ref.Val {
		target, err := asString(target0)
		if err != nil {
			return err
		}
		patterns, err := asStringList(patterns0)
		if err != nil {
			return err
		}
		return boolOrErr(f(strings.ToLower(target), lowerList(patterns)))
	}
}

func wrapL2SCI(f func(targets []string, pattern string) (bool, error)) functions.BinaryOp {
	return func(targets0, pattern0 ref.Val) ref.Val {
		targets, err := asStringList(targets0)
		if err != nil {
			return err
		}
		pattern, err := asString(pattern0)
		if err != nil {
			return err
		}
		return boolOrErr(f(lowerList(targets), strings.ToLower(pattern)))
	}
}

func wrapL2LCI(f func(targets, patterns []string) (bool, error)) functions.BinaryOp {
	return func(targets0, patterns0 ref.Val) ref.Val {
		targets, err := asStringList(targets0)
		if err != nil {
			return err
		}
		patterns, err := asStringList(patterns0)
		if err != nil {
			return err
		}
		return boolOrErr(f(lowerList(targets), lowerList(patterns)))
	}
}

func goExistsEqualsStringToString(target, pattern string) (bool, error) {
	return target == pattern, nil
}

func goExistsEqualsStringToList(target string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		if target == pattern {
			return true, nil
		}
	}
	return false, nil
}

func goExistsEqualsListToString(targets []string, pattern string) (bool, error) {
	for _, target := range targets {
		if target == pattern {
			return true, nil
		}
	}
	return false, nil
}

func makeSet(items []string) map[string]struct{} {
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		set[item] = struct{}{}
	}
	return set
}

func goExistsEqualsListToList(targets []string, patterns []string) (bool, error) {
	set := makeSet(patterns)
	for _, target := range targets {
		if _, has := set[target]; has {
			return true, nil
		}
	}
	return false, nil
}

func goExistsStartsStringToString(target, pattern string) (bool, error) {
	return strings.HasPrefix(target, pattern), nil
}

func goExistsStartsStringToList(target string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		if strings.HasPrefix(target, pattern) {
			return true, nil
		}
	}
	return false, nil
}

func goExistsStartsListToString(targets []string, pattern string) (bool, error) {
	for _, target := range targets {
		if strings.HasPrefix(target, pattern) {
			return true, nil
		}
	}
	return false, nil
}

func goExistsStartsListToList(targets []string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		for _, target := range targets {
			if strings.HasPrefix(target, pattern) {
				return true, nil
			}
		}
	}
	return false, nil
}

func goExistsEndsStringToString(target, pattern string) (bool, error) {
	return strings.HasSuffix(target, pattern), nil
}

func goExistsEndsStringToList(target string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		if strings.HasSuffix(target, pattern) {
			return true, nil
		}
	}
	return false, nil
}

func goExistsEndsListToString(targets []string, pattern string) (bool, error) {
	for _, target := range targets {
		if strings.HasSuffix(target, pattern) {
			return true, nil
		}
	}
	return false, nil
}

func goExistsEndsListToList(targets []string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		for _, target := range targets {
			if strings.HasSuffix(target, pattern) {
				return true, nil
			}
		}
	}
	return false, nil
}

func goExistsContainsStringToString(target, pattern string) (bool, error) {
	return strings.Contains(target, pattern), nil
}

func goExistsContainsStringToList(target string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		if strings.Contains(target, pattern) {
			return true, nil
		}
	}
	return false, nil
}

func goExistsContainsListToString(targets []string, pattern string) (bool, error) {
	for _, target := range targets {
		if strings.Contains(target, pattern) {
			return true, nil
		}
	}
	return false, nil
}

func goExistsContainsListToList(targets []string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		for _, target := range targets {
			if strings.Contains(target, pattern) {
				return true, nil
			}
		}
	}
	return false, nil
}

func goExistsRegexpStringToString(target, pattern string) (bool, error) {
	return regexp.MatchString(pattern, target)
}

func goExistsRegexpStringToList(target string, patterns []string) (bool, error) {
	r, err := regexp.Compile(joinRegexps(patterns, false))
	if err != nil {
		return false, err
	}
	if r.MatchString(target) {
		return true, nil
	}
	return false, nil
}

func goExistsRegexpListToString(targets []string, pattern string) (bool, error) {
	r, err := regexp.Compile(pattern)
	if err != nil {
		return false, err
	}
	for _, target := range targets {
		if r.MatchString(target) {
			return true, nil
		}
	}
	return false, nil
}

func goExistsRegexpListToList(targets []string, patterns []string) (bool, error) {
	r, err := regexp.Compile(joinRegexps(patterns, false))
	if err != nil {
		return false, err
	}
	for _, target := range targets {
		if r.MatchString(target) {
			return true, nil
		}
	}
	return false, nil
}

var analyzer = func() *analysis.DefaultAnalyzer {
	cache := registry.NewCache()
	tokenizer, err := cache.TokenizerNamed(unicode.Name)
	if err != nil {
		panic(err)
	}
	toLowerFilter, err := cache.TokenFilterNamed(lowercase.Name)
	if err != nil {
		panic(err)
	}
	return &analysis.DefaultAnalyzer{
		Tokenizer: tokenizer,
		TokenFilters: []analysis.TokenFilter{
			toLowerFilter,
		},
	}
}()

func tokenize(text string) []string {
	tokens := analyzer.Analyze([]byte(text))
	terms := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == nil {
			terms = append(terms, "")
		} else {
			terms = append(terms, string(token.Term))
		}
	}
	return terms
}

func goExistsContainsTextStringToString(target, pattern string) (bool, error) {
	for _, token := range tokenize(target) {
		if token == pattern {
			return true, nil
		}
	}
	return false, nil
}

func goExistsContainsTextStringToList(target string, patterns []string) (bool, error) {
	set := makeSet(patterns)
	for _, token := range tokenize(target) {
		if _, has := set[token]; has {
			return true, nil
		}
	}
	return false, nil
}

func goExistsContainsTextListToString(targets []string, pattern string) (bool, error) {
	for _, target := range targets {
		for _, token := range tokenize(target) {
			if token == pattern {
				return true, nil
			}
		}
	}
	return false, nil
}

func goExistsContainsTextListToList(targets, patterns []string) (bool, error) {
	set := makeSet(patterns)
	for _, target := range targets {
		for _, token := range tokenize(target) {
			if _, has := set[token]; has {
				return true, nil
			}
		}
	}
	return false, nil
}
