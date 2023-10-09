package consulKVDeserializer

import (
	"errors"
	"fmt"
	capi "github.com/hashicorp/consul/api"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// Deserialize structure from Consul KV
// StructurePtr - pointer to structure
// ConsulRoot - root path in Consul KV
// ConsulKV - Consul KV client created with newConsulKV
// Supported types: string, int, bool, struct, map[string]any
// Use tag "consul" to mark fields
func Deserialize(StructurePtr interface{}, ConsulRoot string, ConsulKV *capi.KV) error {

	err := processStruct(reflect.ValueOf(StructurePtr), ConsulRoot, "consul", ConsulKV)
	if err != nil {
		return errors.Join(err, errors.New("failed to load kv"))
	}

	return nil
}

func processStruct(value reflect.Value, prefix string, tagKey string, kv *capi.KV) error {
	for i := 0; i < value.Elem().NumField(); i++ {

		tag := value.Elem().Type().Field(i).Tag.Get(tagKey)
		if tag == "" {
			continue
		}

		switch {
		case value.Elem().Field(i).Kind() == reflect.Struct:
			err := processStruct(value.Elem().Field(i).Addr(), fmt.Sprintf("%s/%s", prefix, tag), tagKey, kv)
			if err != nil {
				return err
			}
		case value.Elem().Field(i).Kind() == reflect.Map:

			// ----------------------------- TODO -----------------------------

			// Check if it's a map[string]any
			if value.Elem().Field(i).Type().Key().Kind() != reflect.String {
				return errors.New(fmt.Sprintf("map '%s' is not a map[string]any", value.Elem().Type().Field(i).Name))
			}

			// Create new map
			value.Elem().Field(i).Set(reflect.MakeMap(value.Elem().Field(i).Type()))

			// form consul prefix
			mapPrefix := fmt.Sprintf("%s/%s", prefix, tag)

			// Get all keys with prefix
			fullKeys, err := getKeysWithPrefix(kv, mapPrefix)
			if err != nil {
				return err
			}

			var keys []string
			for _, fullKey := range fullKeys {
				key := strings.Split(fullKey[len(mapPrefix+"/"):], "/")[0]

				if !slices.Contains(keys, key) {
					keys = append(keys, key)
				}
			}

			for _, key := range keys {

				// create new value for map
				mapSubStruct := reflect.New(value.Elem().Field(i).Type().Elem())

				// process struct
				err := processStruct(mapSubStruct, fmt.Sprintf("%s/%s", mapPrefix, key), tagKey, kv)
				if err != nil {
					return err
				}

				// add new value to map with key = key
				value.Elem().Field(i).SetMapIndex(reflect.ValueOf(key), mapSubStruct.Elem())
			}

			// ----------------------------- TODO END-----------------------------

		// Ordinary field
		default:

			if value.Elem().Type().Field(i).IsExported() != true {
				return errors.New(fmt.Sprintf("field '%s' is not exported", value.Type().Field(i).Name))
			}

			if value.Elem().Field(i).IsValid() != true {
				return errors.New(fmt.Sprintf("field '%s' is not valid", value.Type().Field(i).Name))
			}

			if value.Elem().Field(i).CanSet() != true {
				return errors.New(fmt.Sprintf("cannot set value for field '%s'", value.Type().Field(i).Name))
			}

			consulValue, err := getTypedConsulValue(kv, fmt.Sprintf("%s/%s", prefix, tag), value.Elem().Field(i).Kind())
			if err != nil {
				return errors.Join(err, errors.New(fmt.Sprintf("failed to get value for field '%s' \n", value.Elem().Type().Field(i).Name)))
			}
			value.Elem().Field(i).Set(reflect.ValueOf(consulValue))

		}
	}
	return nil
}

func getTypedConsulValue(kv *capi.KV, key string, kind reflect.Kind) (any, error) {
	pair, err := getPair(kv, key)
	if err != nil {
		return nil, err
	}

	value := string(pair.Value)

	switch kind {
	case reflect.String:
		return value, nil
	case reflect.Int:
		return strconv.Atoi(value)
	case reflect.Bool:
		return value != "false" && value != "0" && value != "", nil
	default:
		return nil, errors.Join(err, errors.New(fmt.Sprintf("type '%s' is not supported", kind)))
	}
}
