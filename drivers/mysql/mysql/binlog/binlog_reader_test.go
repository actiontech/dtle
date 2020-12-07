package binlog

import "testing"

func Test_loadMapping(t *testing.T) {
	type args struct {
		sql           string
		beforeName    string
		afterName     string
		mappingType   string
		currentSchema string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "schema-rename-drop-table", args: args{
			sql:           "drop table `a`.`c`",
			beforeName:    "a",
			afterName:     "b",
			mappingType:   "schemaRename",
			currentSchema: "",
		}, want: "drop table `b`.`c`"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := loadMapping(tt.args.sql, tt.args.beforeName, tt.args.afterName, tt.args.mappingType, tt.args.currentSchema); got != tt.want {
				t.Errorf("loadMapping() = %v, want %v", got, tt.want)
			}
		})
	}
}
