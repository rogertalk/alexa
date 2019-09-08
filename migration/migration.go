package migration

import (
	"log"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/cloud/datastore"
)

type AlexaAuthData struct {
	AccountId    int64 `datastore:"-"`
	AccessToken  string
	ExpiresIn    int
	LastRefresh  time.Time
	RefreshToken string
	TokenType    string
}

type Transform func(aad AlexaAuthData) (*datastore.Key, interface{})

type job struct {
	keys        []*datastore.Key
	transformer Transform
	values      []interface{}
}

func newJob(t Transform) job {
	return job{
		keys:        make([]*datastore.Key, 0, 1000),
		transformer: t,
		values:      make([]interface{}, 0, 1000),
	}
}

func (j *job) add(aad AlexaAuthData) {
	key, value := j.transformer(aad)
	dupe := false
	for _, k := range j.keys {
		if k.Equal(key) {
			log.Printf("duplicate key: %s", k.String())
			dupe = true
			break
		}
	}
	if dupe {
		return
	}
	j.keys = append(j.keys, key)
	j.values = append(j.values, value)
}

func (j *job) count() int {
	return len(j.values)
}

func Migrate(dest *datastore.Client, transformer Transform) {
	ctx := context.Background()
	client, _ := datastore.NewClient(ctx, "roger-web-client")
	q := datastore.NewQuery("AlexaAuthData")
	it := client.Run(ctx, q)
	batches := make([]job, 0)
	batch := newJob(transformer)
	for {
		var aad AlexaAuthData
		key, err := it.Next(&aad)
		if err == datastore.Done {
			break
		}
		if err != nil {
			log.Fatalf("Error fetching next task: %v", err)
		}
		accountId, _ := strconv.ParseInt(strings.Split(key.Name(), "_")[1], 10, 64)
		aad.AccountId = accountId
		batch.add(aad)
		if batch.count() == 500 {
			batches = append(batches, batch)
			batch = newJob(transformer)
		}
	}
	if batch.count() > 0 {
		batches = append(batches, batch)
	}
	for _, batch := range batches {
		_, err := dest.PutMulti(ctx, batch.keys, batch.values)
		if err != nil {
			log.Fatalf("FAILLLLLLLL %v", err)
		}
		log.Printf("put batch of %d", len(batch.keys))
	}
}
