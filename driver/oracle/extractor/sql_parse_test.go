package extractor

import (
	"testing"
)

func TestColumnsValueConverter(t *testing.T) {
	tests := []struct {
		name          string
		coverageValue string
		expect        string
	}{
		// assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')")).isEqualTo("Вы");

		// // Concatenated values, using differing spacing techniques to validate behavior
		// assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B') || UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
		// assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')||UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
		// assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B') ||UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
		// assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')|| UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
		// assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')||  UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
		// assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')  ||  UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
		{
			name:          "UNISTR",
			coverageValue: `UNISTR(\\0412\\044B) ||UNISTR(\\0431\\0443)`,
			expect:        `Выбу`,
		},
		{
			name:          "UNISTR",
			coverageValue: `UNISTR(\\6570\\636E\\5E93\\6D4B\\8BD5)`,
			expect:        `数据库测试`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actal := columnsValueConverter(tt.coverageValue)
			if actal != tt.expect {
				t.Errorf("UNISTR() actal %v, expect %v", actal, tt.expect)
			}
		})

	}

}
