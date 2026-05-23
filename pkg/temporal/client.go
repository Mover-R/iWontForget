package temporal

import (
	"go.temporal.io/sdk/client"
)

func NewTemporalClient(conf *TemporalConfig) (client.Client, error) {
	client, err := client.NewClient(client.Options{
		HostPort:  conf.Address,
		Namespace: conf.Namespace,
	})
	return client, err
}
