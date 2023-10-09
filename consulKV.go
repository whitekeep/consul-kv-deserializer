package consulKVDeserializer

import (
	"errors"
	"fmt"
	capi "github.com/hashicorp/consul/api"
	"strconv"
)

func newConsulKV(ConsulHost string, ConsulPort int) (*capi.KV, error) {
	consulClient, err := capi.NewClient(&capi.Config{Address: ConsulHost + ":" + strconv.Itoa(ConsulPort)})
	if err != nil {
		return nil, errors.Join(err, errors.New("failed to create consul client"))
	}

	kv := consulClient.KV()

	return kv, nil
}

// TODO кэшировать: можно одним запросом получить все ключи и значения по ConsulRoot
func getPair(kv *capi.KV, key string) (*capi.KVPair, error) {
	pair, _, err := kv.Get(key, nil)
	if err != nil {
		return nil, errors.Join(err, errors.New(fmt.Sprintf("consul: failed to get KV: %s \n", err)))
	}

	if pair == nil {
		return nil, errors.Join(err, errors.New(fmt.Sprintf("consul: failed to get value by key '%s' - pair is not exist", key)))
	}

	return pair, nil
}

func getKeysWithPrefix(kv *capi.KV, prefix string) ([]string, error) {
	pairs, _, err := kv.List(prefix, nil)
	if err != nil {
		return nil, errors.Join(err, errors.New(fmt.Sprintf("consul: failed to get KV: %s \n", err)))
	}

	keys := make([]string, len(pairs))
	for i, pair := range pairs {
		keys[i] = pair.Key
	}

	return keys, nil
}
