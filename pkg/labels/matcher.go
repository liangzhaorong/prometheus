// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package labels

import (
	"fmt"
	"regexp"
)

// MatchType is an enum for label matching types.
type MatchType int

// Possible MatchTypes.
const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (m MatchType) String() string {
	typeToStr := map[MatchType]string{
		MatchEqual:     "=",
		MatchNotEqual:  "!=",
		MatchRegexp:    "=~",
		MatchNotRegexp: "!~",
	}
	if str, ok := typeToStr[m]; ok {
		return str
	}
	panic("unknown match type")
}

// Matcher models the matching of a label.
// Matcher 主要用于匹配时序中指定 Label 的 Value 值, 从而实现过滤时序数据的功能.
type Matcher struct {
	Type  MatchType
	Name  string // 需要匹配的 Label Name 值
	Value string // 需要匹配的 Label Value 值

	re *regexp.Regexp // 指定时序数据中对应 Label Vaelu 的规则, 只有 Label Value 值符合该正则表达式, 时序才能匹配成功
}

// NewMatcher returns a matcher object.
func NewMatcher(t MatchType, n, v string) (*Matcher, error) {
	m := &Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + v + ")$")
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

// MustNewMatcher panics on error - only for use in tests!
func MustNewMatcher(mt MatchType, name, val string) *Matcher {
	m, err := NewMatcher(mt, name, val)
	if err != nil {
		panic(err)
	}
	return m
}

func (m *Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

// Matches returns whether the matcher matches the given string value.
// Matches 检测传入的 Label Value 值是否符合当前 Matcher 的规则
func (m *Matcher) Matches(s string) bool {
	switch m.Type {
	case MatchEqual:
		return s == m.Value
	case MatchNotEqual:
		return s != m.Value
	case MatchRegexp:
		return m.re.MatchString(s)
	case MatchNotRegexp:
		return !m.re.MatchString(s)
	}
	panic("labels.Matcher.Matches: invalid match type")
}

// Inverse returns a matcher that matches the opposite.
func (m *Matcher) Inverse() (*Matcher, error) {
	switch m.Type {
	case MatchEqual:
		return NewMatcher(MatchNotEqual, m.Name, m.Value)
	case MatchNotEqual:
		return NewMatcher(MatchEqual, m.Name, m.Value)
	case MatchRegexp:
		return NewMatcher(MatchNotRegexp, m.Name, m.Value)
	case MatchNotRegexp:
		return NewMatcher(MatchRegexp, m.Name, m.Value)
	}
	panic("labels.Matcher.Matches: invalid match type")
}
