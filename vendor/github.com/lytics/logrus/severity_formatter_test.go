package logrus

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/stretchr/testify/assert"

	"testing"
)

func LogAndAssertSeverityJSON(t *testing.T, log func(*Logger), assertions func(fields Fields)) {
	var buffer bytes.Buffer
	var fields Fields

	logger := New()
	logger.Out = &buffer
	logger.Formatter = new(SeverityFormatter)

	log(logger)

	err := json.Unmarshal(buffer.Bytes(), &fields)
	assert.Nil(t, err)

	assertions(fields)
}

func TestSeverityErrorNotLost(t *testing.T) {
	formatter := &SeverityFormatter{}

	b, err := formatter.Format(WithField("error", errors.New("wild walrus")))
	if err != nil {
		t.Fatal("Unable to format entry: ", err)
	}

	entry := make(map[string]interface{})
	err = json.Unmarshal(b, &entry)
	if err != nil {
		t.Fatal("Unable to unmarshal formatted entry: ", err)
	}

	if entry["error"] != "wild walrus" {
		t.Fatal("Error field not set")
	}
}

func TestSeverityErrorNotLostOnFieldNotNamedError(t *testing.T) {
	formatter := &SeverityFormatter{}

	b, err := formatter.Format(WithField("omg", errors.New("wild walrus")))
	if err != nil {
		t.Fatal("Unable to format entry: ", err)
	}

	entry := make(map[string]interface{})
	err = json.Unmarshal(b, &entry)
	if err != nil {
		t.Fatal("Unable to unmarshal formatted entry: ", err)
	}

	if entry["omg"] != "wild walrus" {
		t.Fatal("Error field not set")
	}
}

func TestSeverityFieldClashWithTime(t *testing.T) {
	formatter := &SeverityFormatter{}

	b, err := formatter.Format(WithField("time", "right now!"))
	if err != nil {
		t.Fatal("Unable to format entry: ", err)
	}

	entry := make(map[string]interface{})
	err = json.Unmarshal(b, &entry)
	if err != nil {
		t.Fatal("Unable to unmarshal formatted entry: ", err)
	}

	if entry["fields.time"] != "right now!" {
		t.Fatal("fields.time not set to original time field")
	}

	if entry["time"] != "0001-01-01T00:00:00Z" {
		t.Fatal("time field not set to current time, was: ", entry["time"])
	}
}

func TestSeverityFieldClashWithMsg(t *testing.T) {
	formatter := &SeverityFormatter{}

	b, err := formatter.Format(WithField("msg", "something"))
	if err != nil {
		t.Fatal("Unable to format entry: ", err)
	}

	entry := make(map[string]interface{})
	err = json.Unmarshal(b, &entry)
	if err != nil {
		t.Fatal("Unable to unmarshal formatted entry: ", err)
	}

	if entry["fields.msg"] != "something" {
		t.Fatal("fields.msg not set to original msg field")
	}
}

func TestSeverityFieldClashWithLevel(t *testing.T) {
	formatter := &SeverityFormatter{}

	b, err := formatter.Format(WithField("level", "something"))
	if err != nil {
		t.Fatal("Unable to format entry: ", err)
	}

	entry := make(map[string]interface{})
	err = json.Unmarshal(b, &entry)
	if err != nil {
		t.Fatal("Unable to unmarshal formatted entry: ", err)
	}

	if entry["fields.level"] != "something" {
		t.Fatal("fields.level not set to original level field")
	}
}

func TestSeverityEntryEndsWithNewline(t *testing.T) {
	formatter := &SeverityFormatter{}

	b, err := formatter.Format(WithField("level", "something"))
	if err != nil {
		t.Fatal("Unable to format entry: ", err)
	}

	if b[len(b)-1] != '\n' {
		t.Fatal("Expected Severity log entry to end with a newline")
	}
}

func TestSeverityPrint(t *testing.T) {
	LogAndAssertSeverityJSON(t, func(log *Logger) {
		log.Print("test")
	}, func(fields Fields) {
		assert.Equal(t, fields["message"], "test")
		assert.Equal(t, fields["level"], "info")
	})
}

func TestSeverityInfo(t *testing.T) {
	LogAndAssertSeverityJSON(t, func(log *Logger) {
		log.Info("test")
	}, func(fields Fields) {
		t.Logf("fields: %#v", fields)
		assert.Equal(t, fields["message"], "test")
		assert.Equal(t, fields["level"], "info")
	})
}

func TestSeverityWarn(t *testing.T) {
	LogAndAssertSeverityJSON(t, func(log *Logger) {
		log.Warn("test")
	}, func(fields Fields) {
		assert.Equal(t, fields["message"], "test")
		assert.Equal(t, fields["level"], "warning")
	})
}
