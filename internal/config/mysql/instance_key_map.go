package mysql

import (
	"encoding/json"
	"strings"
)

// InstanceKeyMap is a convenience struct for listing InstanceKey-s
type InstanceKeyMap map[InstanceKey]bool

func NewInstanceKeyMap() *InstanceKeyMap {
	return &InstanceKeyMap{}
}

func (i *InstanceKeyMap) Len() int {
	return len(*i)
}

// AddKey adds a single key to this map
func (i *InstanceKeyMap) AddKey(key InstanceKey) {
	(*i)[key] = true
}

// AddKeys adds all given keys to this map
func (i *InstanceKeyMap) AddKeys(keys []InstanceKey) {
	for _, key := range keys {
		i.AddKey(key)
	}
}

// HasKey checks if given key is within the map
func (i *InstanceKeyMap) HasKey(key InstanceKey) bool {
	_, ok := (*i)[key]
	return ok
}

// GetInstanceKeys returns keys in this map in the form of an array
func (i *InstanceKeyMap) GetInstanceKeys() []InstanceKey {
	res := []InstanceKey{}
	for key := range *i {
		res = append(res, key)
	}
	return res
}

// MarshalJSON will marshal this map as JSON
func (i *InstanceKeyMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.GetInstanceKeys())
}

// ToJSON will marshal this map as JSON
func (i *InstanceKeyMap) ToJSON() (string, error) {
	bytes, err := i.MarshalJSON()
	return string(bytes), err
}

// ToJSONString will marshal this map as JSON
func (i *InstanceKeyMap) ToJSONString() string {
	s, _ := i.ToJSON()
	return s
}

// ToCommaDelimitedList will export this map in comma delimited format
func (i *InstanceKeyMap) ToCommaDelimitedList() string {
	keyDisplays := []string{}
	for key := range *i {
		keyDisplays = append(keyDisplays, key.DisplayString())
	}
	return strings.Join(keyDisplays, ",")
}

// ReadJson unmarshalls a json into this map
func (i *InstanceKeyMap) ReadJson(jsonString string) error {
	var keys []InstanceKey
	err := json.Unmarshal([]byte(jsonString), &keys)
	if err != nil {
		return err
	}
	i.AddKeys(keys)
	return err
}

// ReadJson unmarshalls a json into this map
func (i *InstanceKeyMap) ReadCommaDelimitedList(list string) error {
	if list == "" {
		return nil
	}
	tokens := strings.Split(list, ",")
	for _, token := range tokens {
		key, err := ParseRawInstanceKeyLoose(token)
		if err != nil {
			return err
		}
		i.AddKey(*key)
	}
	return nil
}
