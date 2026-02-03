package cel2sql_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockscomb/cel2sql"
	"github.com/cockscomb/cel2sql/bq"
	"github.com/cockscomb/cel2sql/filters"
	"github.com/cockscomb/cel2sql/sqltypes"
	"github.com/cockscomb/cel2sql/test"
	"github.com/google/cel-go/ext"
)

func TestConvert(t *testing.T) {
	env, err := cel.NewEnv(
		ext.Strings(),
		cel.EnableMacroCallTracking(),
		sqltypes.AdditionalMacros,
		cel.CustomTypeProvider(bq.NewTypeProvider(map[string]bigquery.Schema{
			"trigrams":  test.NewTrigramsTableMetadata().Schema,
			"wikipedia": test.NewWikipediaTableMetadata().Schema,
		})),
		sqltypes.SQLTypeDeclarations,
		cel.Declarations(
			decls.NewVar("name", decls.String),
			decls.NewVar("age", decls.Int),
			decls.NewVar("adult", decls.Bool),
			decls.NewVar("height", decls.Double),
			decls.NewVar("string_list", decls.NewListType(decls.String)),
			decls.NewVar("string_int_map", decls.NewMapType(decls.String, decls.Int)),
			decls.NewVar("nullable_string", decls.NewWrapperType(decls.String)),
			decls.NewVar("nullable_bytes", decls.NewWrapperType(decls.Bytes)),
			decls.NewVar("nullable_strings", decls.NewListType(decls.NewWrapperType(decls.String))),
			decls.NewVar("null_var", decls.Null),
			decls.NewVar("birthday", sqltypes.Date),
			decls.NewVar("fixed_time", sqltypes.Time),
			decls.NewVar("scheduled_at", sqltypes.DateTime),
			decls.NewVar("created_at", decls.Timestamp),
			decls.NewVar("trigram", decls.NewObjectType("trigrams")),
			decls.NewVar("page", decls.NewObjectType("wikipedia")),
			decls.NewVar("pages", decls.NewListType(decls.NewObjectType("wikipedia"))),
		),
		filters.Declarations,
	)
	require.NoError(t, err)
	type args struct {
		source string
	}
	tests := []struct {
		name                 string
		args                 args
		maxArgumentsToExpand int
		want                 string
		wantCompileErr       bool
		wantErr              bool
		idents               []string
		options              []cel2sql.ConvertOption
	}{
		{
			name: "startsWith",
			args: args{source: `name.startsWith("a")`},
			want: "STARTS_WITH(`name`, \"a\")",
		},
		{
			name: "endsWith",
			args: args{source: `name.endsWith("z")`},
			want: "ENDS_WITH(`name`, \"z\")",
		},
		{
			name: "matches",
			args: args{source: `name.matches("a+")`},
			want: "REGEXP_CONTAINS(`name`, \"a+\")",
		},
		{
			name: "contains",
			args: args{source: `name.contains("abc")`},
			want: "STRPOS(`name`, \"abc\") != 0",
		},
		{
			name: "replace",
			args: args{source: `name.replace("abc", "def")`},
			want: "REPLACE(`name`, \"abc\", \"def\")",
		},
		{
			name: "&&",
			args: args{source: `name.startsWith("a") && name.endsWith("z")`},
			want: "STARTS_WITH(`name`, \"a\") AND ENDS_WITH(`name`, \"z\")",
		},
		{
			name: "||",
			args: args{source: `name.startsWith("a") || name.endsWith("z")`},
			want: "STARTS_WITH(`name`, \"a\") OR ENDS_WITH(`name`, \"z\")",
		},
		{
			name: "()",
			args: args{source: `age >= 10 && (name.startsWith("a") || name.endsWith("z"))`},
			want: "`age` >= 10 AND (STARTS_WITH(`name`, \"a\") OR ENDS_WITH(`name`, \"z\"))",
		},
		{
			name: "IF",
			args: args{source: `name == "a" ? "a" : "b"`},
			want: "IF(`name` = \"a\", \"a\", \"b\")",
		},
		{
			name: "==",
			args: args{source: `name == "a"`},
			want: "`name` = \"a\"",
		},
		{
			name: "!=",
			args: args{source: `age != 20`},
			want: "`age` != 20",
		},
		{
			name: "IS NULL",
			args: args{source: `null_var == null`},
			want: "`null_var` IS NULL",
		},
		{
			name: "IS NOT TRUE",
			args: args{source: `adult != true`},
			want: "`adult` IS NOT TRUE",
		},
		{
			name: "<",
			args: args{source: `age < 20`},
			want: "`age` < 20",
		},
		{
			name: ">=",
			args: args{source: `height >= 1.6180339887`},
			want: "`height` >= 1.6180339887",
		},
		{
			name: "NOT",
			args: args{source: `!adult`},
			want: "NOT `adult`",
		},
		{
			name: "-",
			args: args{source: `-1`},
			want: "-1",
		},
		{
			name: "list",
			args: args{source: `[1, 2, 3][0] == 1`},
			want: "[1, 2, 3][OFFSET(0)] = 1",
		},
		{
			name: "list_var",
			args: args{source: `string_list[0] == "a"`},
			want: "`string_list`[OFFSET(0)] = \"a\"",
		},
		{
			name: "safe_list",
			args: args{source: `[1, 2, 3].get(0) == 1`},
			want: "[1, 2, 3][SAFE_OFFSET(0)] = 1",
		},
		{
			name: "safe_list_var",
			args: args{source: `string_list.get(0) == "a"`},
			want: "`string_list`[SAFE_OFFSET(0)] = \"a\"",
		},
		{
			name: "map",
			args: args{source: `{"one": 1, "two": 2, "three": 3}["one"] == 1`},
			want: "STRUCT(1 AS one, 2 AS two, 3 AS three).`one` = 1",
		},
		{
			name: "map_var",
			args: args{source: `string_int_map["one"] == 1`},
			want: "`string_int_map`.`one` = 1",
		},
		{
			name:    "invalidFieldType",
			args:    args{source: `{1: 1}[1]`},
			want:    "",
			wantErr: true,
		},
		{
			name:    "invalidFieldName",
			args:    args{source: `{"on e": 1}["on e"]`},
			want:    "",
			wantErr: true,
		},
		{
			name: "add",
			args: args{source: `1 + 2 == 3`},
			want: "1 + 2 = 3",
		},
		{
			name: "concatString",
			args: args{source: `"a" + "b" == "ab"`},
			want: `"a" || "b" = "ab"`,
		},
		{
			name: "nullable_string_isNull",
			args: args{source: `nullable_string == null`},
			want: "`nullable_string` IS NULL",
		},
		{
			name: "nullable_string_equals",
			args: args{source: `nullable_string == "hello"`},
			want: "`nullable_string` = \"hello\"",
		},
		{
			name: "nullable_string_concat",
			args: args{source: `nullable_string + "hello"`},
			want: "`nullable_string` || \"hello\"",
		},
		{
			name: "nullable_bytes_isNull",
			args: args{source: `nullable_bytes == null`},
			want: "`nullable_bytes` IS NULL",
		},
		{
			name: "nullable_bytes_equals",
			args: args{source: `nullable_bytes == b"hello"`},
			want: "`nullable_bytes` = b\"\\150\\145\\154\\154\\157\"",
		},
		{
			name: "nullable_bytes_concat",
			args: args{source: `nullable_bytes + b"hello"`},
			want: "`nullable_bytes` || b\"\\150\\145\\154\\154\\157\"",
		},
		{
			name:   "nullable_strings_containsNull",
			args:   args{source: `nullable_strings.exists(x, x == null)`},
			want:   "EXISTS (SELECT * FROM UNNEST(`nullable_strings`) AS x WHERE `x` IS NULL)",
			idents: []string{"nullable_strings"},
		},
		{
			name:   "nullable_strings_containsEquals",
			args:   args{source: `nullable_strings.exists(x, x.existsEqualsCI(["hello"]))`},
			want:   "EXISTS (SELECT * FROM UNNEST(`nullable_strings`) AS x WHERE COLLATE(`x`, \"und:ci\") = \"hello\")",
			idents: []string{"nullable_strings"},
		},
		{
			name: "concatList",
			args: args{source: `1 in [1] + [2, 3]`},
			want: "1 IN UNNEST([1] || [2, 3])",
		},
		{
			name: "modulo",
			args: args{source: `5 % 3 == 2`},
			want: "MOD(5, 3) = 2",
		},
		{
			name: "date",
			args: args{source: `birthday > date(2000, 1, 1) + 1`},
			want: "`birthday` > DATE(2000, 1, 1) + 1",
		},
		{
			name: "time",
			args: args{source: `fixed_time == time("18:00:00")`},
			want: "`fixed_time` = TIME(\"18:00:00\")",
		},
		{
			name: "datetime",
			args: args{source: `scheduled_at != datetime(date("2021-09-01"), fixed_time)`},
			want: "`scheduled_at` != DATETIME(DATE(\"2021-09-01\"), `fixed_time`)",
		},
		{
			name: "null_timestamp",
			args: args{source: `created_at == timestamp(0) && created_at != timestamp(0)`},
			want: "`created_at` IS NULL AND `created_at` IS NOT NULL",
		},
		{
			name: "has_timestamp",
			args: args{source: `has(page.timestamp) && !has(page.timestamp)`},
			want: "`page`.`timestamp` IS NOT NULL AND NOT `page`.`timestamp` IS NOT NULL",
		},
		{
			name: "timestamp",
			args: args{source: `created_at - duration("60m") <= timestamp(datetime("2021-09-01 18:00:00"), "Asia/Tokyo")`},
			want: "TIMESTAMP_SUB(`created_at`, INTERVAL 1 HOUR) <= TIMESTAMP(DATETIME(\"2021-09-01 18:00:00\"), \"Asia/Tokyo\")",
		},
		{
			name: "duration_second",
			args: args{source: `duration("10s")`},
			want: "INTERVAL 10 SECOND",
		},
		{
			name: "duration_minute",
			args: args{source: `duration("1h1m")`},
			want: "INTERVAL 61 MINUTE",
		},
		{
			name: "duration_hour",
			args: args{source: `duration("60m")`},
			want: "INTERVAL 1 HOUR",
		},
		{
			name: "interval",
			args: args{source: `interval(1, MONTH)`},
			want: "INTERVAL 1 MONTH",
		},
		{
			name: "date_add",
			args: args{source: `date("2021-09-01") + interval(1, DAY)`},
			want: `DATE_ADD(DATE("2021-09-01"), INTERVAL 1 DAY)`,
		},
		{
			name: "date_sub",
			args: args{source: `current_date() - interval(1, DAY)`},
			want: "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
		},
		{
			name: "time_add",
			args: args{source: `time("09:00:00") + interval(1, MINUTE)`},
			want: `TIME_ADD(TIME("09:00:00"), INTERVAL 1 MINUTE)`,
		},
		{
			name: "time_sub",
			args: args{source: `time("09:00:00") - interval(1, MINUTE)`},
			want: `TIME_SUB(TIME("09:00:00"), INTERVAL 1 MINUTE)`,
		},
		{
			name: "datetime_add",
			args: args{source: `datetime("2021-09-01 18:00:00") + interval(1, MINUTE)`},
			want: `DATETIME_ADD(DATETIME("2021-09-01 18:00:00"), INTERVAL 1 MINUTE)`,
		},
		{
			name: "datetime_sub",
			args: args{source: `current_datetime("Asia/Tokyo") - interval(1, MINUTE)`},
			want: `DATETIME_SUB(CURRENT_DATETIME("Asia/Tokyo"), INTERVAL 1 MINUTE)`,
		},
		{
			name: "timestamp_add",
			args: args{source: `duration("1h") + timestamp("2021-09-01T18:00:00Z")`},
			want: `TIMESTAMP_ADD(TIMESTAMP("2021-09-01T18:00:00Z"), INTERVAL 1 HOUR)`,
		},
		{
			name: "timestamp_sub",
			args: args{source: `created_at - interval(1, HOUR)`},
			want: "TIMESTAMP_SUB(`created_at`, INTERVAL 1 HOUR)",
		},
		{
			name: "timestamp_getSeconds",
			args: args{source: `created_at.getSeconds()`},
			want: "EXTRACT(SECOND FROM `created_at`)",
		},
		{
			name: "\"timestamp_getHours_withTimezone",
			args: args{source: `created_at.getHours("Asia/Tokyo")`},
			want: "EXTRACT(HOUR FROM `created_at` AT \"Asia/Tokyo\")",
		},
		{
			name: "date_getFullYear",
			args: args{source: `birthday.getFullYear()`},
			want: "EXTRACT(YEAR FROM `birthday`)",
		},
		{
			name: "datetime_getMonth",
			args: args{source: `scheduled_at.getMonth()`},
			want: "EXTRACT(MONTH FROM `scheduled_at`) - 1",
		},
		{
			name: "datetime_getDayOfMonth",
			args: args{source: `scheduled_at.getDayOfMonth()`},
			want: "EXTRACT(DAY FROM `scheduled_at`) - 1",
		},
		{
			name: "time_getMinutes",
			args: args{source: `fixed_time.getMinutes()`},
			want: "EXTRACT(MINUTE FROM `fixed_time`)",
		},
		{
			name: "date_trunc",
			args: args{source: `date("2023-01-01").trunc(DAY)`},
			want: `DATE_TRUNC(DATE("2023-01-01"), DAY)`,
		},
		{
			name: "time_trunc",
			args: args{source: `time("18:00:00").trunc(HOUR)`},
			want: `TIME_TRUNC(TIME("18:00:00"), HOUR)`,
		},
		{
			name: "datetime_trunc",
			args: args{source: `datetime("2023-09-01 18:00:00").trunc(MINUTE)`},
			want: `DATETIME_TRUNC(DATETIME("2023-09-01 18:00:00"), MINUTE)`,
		},
		{
			name: "timestamp_trunc",
			args: args{source: `timestamp("2023-09-01 18:00:00").trunc(WEEK)`},
			want: `TIMESTAMP_TRUNC(TIMESTAMP("2023-09-01 18:00:00"), WEEK)`,
		},
		{
			name:   "fieldSelect",
			args:   args{source: `page.title == "test"`},
			want:   "`page`.`title` = \"test\"",
			idents: []string{"page.title"},
		},
		{
			name:   "fieldSelect_startsWith",
			args:   args{source: `page.title.startsWith("test")`},
			want:   "STARTS_WITH(`page`.`title`, \"test\")",
			idents: []string{"page.title"},
		},
		{
			name:   "fieldSelect_add",
			args:   args{source: `trigram.cell[0].page_count + 1`},
			want:   "`trigram`.`cell`[OFFSET(0)].`page_count` + 1",
			idents: []string{"trigram.cell", ".page_count"},
		},
		{
			name:   "fieldSelect_concatString",
			args:   args{source: `trigram.cell[0].sample[0].title + "test"`},
			want:   "`trigram`.`cell`[OFFSET(0)].`sample`[OFFSET(0)].`title` || \"test\"",
			idents: []string{"trigram.cell", ".sample", ".title"},
		},
		{
			name:   "fieldSelect_in",
			args:   args{source: `"test" in trigram.cell[0].value`},
			want:   "\"test\" IN UNNEST(`trigram`.`cell`[OFFSET(0)].`value`)",
			idents: []string{"trigram.cell", ".value"},
		},
		{
			name:   "safe_fieldSelect_add",
			args:   args{source: `trigram.cell.get(0).page_count + 1`},
			want:   "`trigram`.`cell`[SAFE_OFFSET(0)].`page_count` + 1",
			idents: []string{"trigram.cell", ".page_count"},
		},
		{
			name:   "safe_fieldSelect_concatString",
			args:   args{source: `trigram.cell.get(0).sample.get(0).title + "test"`},
			want:   "`trigram`.`cell`[SAFE_OFFSET(0)].`sample`[SAFE_OFFSET(0)].`title` || \"test\"",
			idents: []string{"trigram.cell", ".sample", ".title"},
		},
		{
			name:   "safe_fieldSelect_in",
			args:   args{source: `"test" in trigram.cell.get(0).value`},
			want:   "\"test\" IN UNNEST(`trigram`.`cell`[SAFE_OFFSET(0)].`value`)",
			idents: []string{"trigram.cell", ".value"},
		},
		{
			name: "cast_bool",
			args: args{source: `bool(0) == false`},
			want: "CAST(0 AS BOOL) IS FALSE",
		},
		{
			name: "cast_bytes",
			args: args{source: `bytes("test")`},
			want: `CAST("test" AS BYTES)`,
		},
		{
			name: "cast_int",
			args: args{source: `int(true) == 1`},
			want: "CAST(TRUE AS INT64) = 1",
		},
		{
			name: "cast_string",
			args: args{source: `string(true) == "true"`},
			want: `CAST(TRUE AS STRING) = "true"`,
		},
		{
			name: "cast_string_from_timestamp",
			args: args{source: `string(created_at)`},
			want: "CAST(`created_at` AS STRING)",
		},
		{
			name: "cast_int_epoch",
			args: args{source: `int(created_at)`},
			want: "UNIX_SECONDS(`created_at`)",
		},
		{
			name: "size_string",
			args: args{source: `size("test")`},
			want: `LENGTH("test")`,
		},
		{
			name: "size_nullable_string",
			args: args{source: `size(nullable_string)`},
			want: "LENGTH(`nullable_string`)",
		},
		{
			name: "size_bytes",
			args: args{source: `size(bytes("test"))`},
			want: `LENGTH(CAST("test" AS BYTES))`,
		},
		{
			name: "size_nullable_bytes",
			args: args{source: `size(nullable_bytes)`},
			want: "LENGTH(`nullable_bytes`)",
		},
		{
			name: "size_list",
			args: args{source: `size(string_list)`},
			want: "ARRAY_LENGTH(`string_list`)",
		},
		{
			name:   "inplace_array_exists",
			args:   args{source: `["foo", "bar"].exists(x, x == "foo")`},
			want:   "EXISTS (SELECT * FROM UNNEST([\"foo\", \"bar\"]) AS x WHERE `x` = \"foo\")",
			idents: []string{},
		},
		{
			name: "filters_exists_equals",
			args: args{source: `"foo".existsEquals("bar") && "foo".existsEquals(["bar"]) && ["foo"].existsEquals("bar") && ["foo"].existsEquals(["bar"])`},
			want: `"foo" = "bar" AND "foo" = "bar" AND "bar" IN UNNEST(["foo"]) AND "bar" IN UNNEST(["foo"])`,
		},
		{
			name: "filters_exists_equals_many",
			args: args{source: `["a"].existsEquals(["b1", "b2"]) && ["a"].existsEquals(["b1", "b2", "b3"]) && ["a"].existsEquals(["b1", "b2", "b3", "b4"])`},
			want: `(("b1" IN UNNEST(["a"])) OR ("b2" IN UNNEST(["a"]))) AND (("b1" IN UNNEST(["a"])) OR ("b2" IN UNNEST(["a"])) OR ("b3" IN UNNEST(["a"]))) AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["a"], "\x00") || "\x00", "\x00(b1|b2|b3|b4)\x00")`,
		},
		{
			name: "filters_exists_equals_many_ci",
			args: args{source: `["a"].existsEqualsCI(["b1", "b2"]) && ["a"].existsEqualsCI(["b1", "b2", "b3"]) && ["a"].existsEqualsCI(["b1", "b2", "b3", "b4"])`},
			want: `((COLLATE("b1", "und:ci") IN UNNEST(["a"])) OR (COLLATE("b2", "und:ci") IN UNNEST(["a"]))) AND ((COLLATE("b1", "und:ci") IN UNNEST(["a"])) OR (COLLATE("b2", "und:ci") IN UNNEST(["a"])) OR (COLLATE("b3", "und:ci") IN UNNEST(["a"]))) AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["a"], "\x00") || "\x00", "(?i)\x00(b1|b2|b3|b4)\x00")`,
		},
		{
			name:                 "filters_exists_equals_many_custom_maxArgumentsToExpand",
			args:                 args{source: `["a"].existsEquals(["b1", "b2"]) && ["a"].existsEquals(["b1", "b2", "b3"]) && ["a"].existsEquals(["b1", "b2", "b3", "b4"])`},
			want:                 `(("b1" IN UNNEST(["a"])) OR ("b2" IN UNNEST(["a"]))) AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["a"], "\x00") || "\x00", "\x00(b1|b2|b3)\x00") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["a"], "\x00") || "\x00", "\x00(b1|b2|b3|b4)\x00")`,
			maxArgumentsToExpand: 2,
		},
		{
			name: "filters_exists_equals_ci",
			args: args{source: `"foo".existsEqualsCI("bar") && "foo".existsEqualsCI(["bar"]) && ["foo"].existsEqualsCI("bar") && ["foo"].existsEqualsCI(["bar"])`},
			want: `COLLATE("foo", "und:ci") = "bar" AND COLLATE("foo", "und:ci") = "bar" AND COLLATE("bar", "und:ci") IN UNNEST(["foo"]) AND COLLATE("bar", "und:ci") IN UNNEST(["foo"])`,
		},
		{
			name: "filters_exists_equals_ci (spanner option)",
			args: args{source: `"foo".existsEqualsCI("bar") && "foo".existsEqualsCI(["bar"]) && ["foo"].existsEqualsCI("bar") && ["foo"].existsEqualsCI(["bar"])`},
			want: `LOWER("foo") = LOWER("bar") AND LOWER("foo") = LOWER("bar") AND LOWER("bar") IN UNNEST(["foo"]) AND LOWER("bar") IN UNNEST(["foo"])`,
			options: []cel2sql.ConvertOption{
				cel2sql.WithSQLDialect(cel2sql.SpannerSQL),
			},
		},
		{
			name: "filters_exists_regexp",
			args: args{source: `"foo".existsRegexp("bar") && "foo".existsRegexp(["bar"]) && ["foo"].existsRegexp("bar") && ["foo"].existsRegexp(["bar"])`},
			want: `REGEXP_CONTAINS("foo", "(bar)") AND REGEXP_CONTAINS("foo", "(bar)") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["foo"], "\x00") || "\x00", "(bar)") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["foo"], "\x00") || "\x00", "(bar)")`,
		},
		{
			name: "filters_exists_regexp_ci",
			args: args{source: `"foo".existsRegexpCI("bar") && "foo".existsRegexpCI(["^bar$"]) && ["foo"].existsRegexpCI("^bar") && ["foo"].existsRegexpCI(["bar$"])`},
			want: `REGEXP_CONTAINS("foo", "(?i)(bar)") AND REGEXP_CONTAINS("foo", "(?i)(^bar$)") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["foo"], "\x00") || "\x00", "(?i)(\x00bar)") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["foo"], "\x00") || "\x00", "(?i)(bar\x00)")`,
		},
		{
			name: "filters_exists_regexp_many_patterns",
			args: args{source: `"foo".existsRegexp(["bar", "zoo"]) && ["foo"].existsRegexp(["^bar$", "^zoo$"])`},
			want: `REGEXP_CONTAINS("foo", "((bar)|(zoo))") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["foo"], "\x00") || "\x00", "((\x00bar\x00)|(\x00zoo\x00))")`,
		},
		{
			name: "filters_exists_regexp_many_patterns_ci",
			args: args{source: `"foo".existsRegexpCI(["^bar", "^zoo"]) && ["foo"].existsRegexpCI(["^bar$", "zoo"])`},
			want: `REGEXP_CONTAINS("foo", "(?i)((^bar)|(^zoo))") AND REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(["foo"], "\x00") || "\x00", "(?i)((\x00bar\x00)|(zoo))")`,
		},
		{
			name:           "filters_no_args",
			args:           args{source: `"foo".existsEquals() && "foo".existsStartsCI() && ["foo"].existsEnds() && ["foo"].existsContainsCI() && "foo".existsRegexp()`},
			wantCompileErr: true,
		},
		{
			name: "filters_empty_array_args",
			args: args{source: `"foo".existsEqualsCI([]) && "foo".existsStarts([]) && ["foo"].existsEndsCI([]) && ["foo"].existsContains([]) && "foo".existsRegexpCI([])`},
			want: "FALSE AND FALSE AND FALSE AND FALSE AND FALSE",
		},
		{
			name:   "map",
			args:   args{source: `pages.map(p, p.title)`},
			want:   "ARRAY(SELECT `p`.`title` FROM `pages` AS p)",
			idents: []string{"pages"},
		},
		{
			name:   "two_level_map",
			args:   args{source: `pages.map(p, ["Title1", "Title2"].map(t, p.title + " " + t))`},
			want:   "ARRAY(SELECT ARRAY(SELECT `p`.`title` || \" \" || `t` FROM [\"Title1\", \"Title2\"] AS t) FROM `pages` AS p)",
			idents: []string{"pages"},
		},
		{
			name:   "mapFilter",
			args:   args{source: `pages.map(p, p.language == "english", p.title)`},
			want:   "ARRAY(SELECT `p`.`title` FROM `pages` AS p WHERE `p`.`language` = \"english\")",
			idents: []string{"pages"},
		},
		{
			name:   "mapDistinct",
			args:   args{source: `pages.mapDistinct(p, p.title)`},
			want:   "ARRAY(SELECT DISTINCT `p`.`title` FROM `pages` AS p)",
			idents: []string{"pages"},
		},
		{
			name:   "mapDistinctFilter",
			args:   args{source: `pages.mapDistinct(p, p.language == "english", p.title)`},
			want:   "ARRAY(SELECT DISTINCT `p`.`title` FROM `pages` AS p WHERE `p`.`language` = \"english\")",
			idents: []string{"pages"},
		},
		{
			name:   "filter",
			args:   args{source: `pages.filter(p, p.language == "english")`},
			want:   "ARRAY(SELECT p FROM `pages` AS p WHERE `p`.`language` = \"english\")",
			idents: []string{"pages"},
		},
	}

	tracker := bq.NewBigQueryNamedTracker()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.args.source)
			if tt.wantCompileErr {
				require.NotEmpty(t, issues)
				return
			}
			require.Empty(t, issues)

			var extOpts []filters.ExtensionOption
			if tt.maxArgumentsToExpand != 0 {
				extOpts = append(extOpts, filters.WithMaxArgumentsToExpand(tt.maxArgumentsToExpand))
			}
			ext := filters.NewExtension(extOpts...)

			identTracker := identTracker(make(map[string]struct{}))
			options := []cel2sql.ConvertOption{
				cel2sql.WithExtension(ext), cel2sql.WithIdentTracker(identTracker),
			}
			options = append(options, tt.options...)
			got, err := cel2sql.Convert(ast, options...)
			if len(tt.idents) != 0 {
				observedIdents := make([]string, 0, len(identTracker))
				for ident := range identTracker {
					observedIdents = append(observedIdents, ident)
				}
				assert.ElementsMatch(t, tt.idents, observedIdents)
			}
			if !tt.wantErr && assert.NoError(t, err) {
				assert.Equal(t, tt.want, got)
			} else {
				assert.Error(t, err)
			}

			t.Run("WithValueTracker", func(t *testing.T) {
				options := []cel2sql.ConvertOption{
					cel2sql.WithValueTracker(tracker), cel2sql.WithExtension(ext),
				}
				options = append(options, tt.options...)
				got, err := cel2sql.Convert(ast, options...)
				for _, v := range tracker.Values {
					got = strings.ReplaceAll(got, "@"+v.Name, cel2sql.ValueToString(v.Value))
				}
				if !tt.wantErr && assert.NoError(t, err) {
					assert.Equal(t, tt.want, got)
				} else {
					assert.Error(t, err)
				}
			})
		})
	}
}

type identTracker map[string]struct{}

func (t identTracker) AddIdentAccess(rootExpr *cel2sql.Expr, path []string) (res []string) {
	if rootExpr == nil {
		t[strings.Join(path, ".")] = struct{}{}
	} else {
		t["."+strings.Join(path, ".")] = struct{}{}
	}
	return path
}
