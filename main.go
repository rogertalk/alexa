package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	// "github.com/fika-io/alexa/migration"
	"github.com/fika-io/go-avs"
	"golang.org/x/net/context"
	"google.golang.org/cloud/datastore"
)

const (
	ConfigPath = "./config.json"
)

var (
	// TODO: Replace cache with an LRU once we start hitting tens of thousands.
	cache      = make(map[int64]*ServiceAuth)
	cacheMutex sync.Mutex
	config     Config
	ctx        = context.Background()
	store      *datastore.Client
)

func main() {
	// Load configuration from file.
	data, err := ioutil.ReadFile(ConfigPath)
	if err != nil {
		log.Fatalf("! Failed to load config (ioutil.ReadFile: %v)", err)
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("! Failed to load config (json.Unmarshal: %v)", err)
	}

	// Set up the Datastore client.
	store, err = datastore.NewClient(ctx, config.ProjectId)
	if err != nil {
		log.Fatalf("! Failed to create Datastore client (datastore.NewClient: %v)", err)
	}

	// migration.Migrate(store, func(aad migration.AlexaAuthData) (*datastore.Key, interface{}) {
	// 	key := Key(ctx, aad.AccountId)
	// 	ia := &ServiceAuth{
	// 		AccountId:    aad.AccountId,
	// 		AccessToken:  aad.AccessToken,
	// 		ExpiresIn:    aad.ExpiresIn,
	// 		LastRefresh:  aad.LastRefresh,
	// 		RefreshToken: aad.RefreshToken,
	// 		Service:      datastore.NewKey(ctx, "Service", config.ServiceId, 0, nil),
	// 		TokenType:    aad.TokenType,
	// 	}
	// 	return key, ia
	// })

	// Set up server for handling incoming requests.
	http.HandleFunc("/request", requestHandler)

	log.Printf(". Starting server on %s...", config.ListenAddr)
	if err := http.ListenAndServe(config.ListenAddr, nil); err != nil {
		log.Fatalf("! Failed to serve (http.ListenAndServe: %v)", err)
	}
}

type Config struct {
	ListenAddr         string
	ProjectId          string
	ServiceId          string
	AmazonClientId     string
	AmazonClientSecret string
}

type ServiceAuth struct {
	AccountId    int64          `datastore:"-" json:"-"`
	AccessToken  string         `datastore:"access_token,noindex" json:"access_token"`
	ExpiresIn    int            `datastore:"expires_in,noindex" json:"expires_in"`
	LastRefresh  time.Time      `datastore:"last_refresh" json:"-"`
	RefreshToken string         `datastore:"refresh_token,noindex" json:"refresh_token"`
	Service      *datastore.Key `datastore:"service" json:"-"`
	TokenType    string         `datastore:"token_type,noindex" json:"token_type"`
}

func (ia *ServiceAuth) Expired() bool {
	return time.Now().After(ia.LastRefresh.Add(time.Duration(ia.ExpiresIn) * time.Second))
}

func (ia *ServiceAuth) Refresh() error {
	resp, err := http.PostForm("https://api.amazon.com/auth/o2/token", url.Values{
		"client_id":     {config.AmazonClientId},
		"client_secret": {config.AmazonClientSecret},
		"grant_type":    {"refresh_token"},
		"refresh_token": {ia.RefreshToken},
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, ia)
}

func Key(ctx context.Context, accountId int64) *datastore.Key {
	return datastore.NewKey(
		ctx, "ServiceAuth", fmt.Sprintf("service_%s", config.ServiceId), 0,
		datastore.NewKey(ctx, "Account", "", accountId, nil))
}

func GetAuth(ctx context.Context, accountId int64) (auth *ServiceAuth, err error) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	auth = cache[accountId]
	if auth == nil || auth.Expired() {
		auth, err = GetAuthFromDatastore(ctx, accountId)
		if err == nil {
			cache[accountId] = auth
		}
	}
	return
}

func GetAuthFromDatastore(ctx context.Context, accountId int64) (*ServiceAuth, error) {
	key := Key(ctx, accountId)
	var auth ServiceAuth
	if err := store.Get(ctx, key, &auth); err != nil {
		return nil, err
	}
	auth.AccountId = accountId
	if auth.Expired() {
		err := auth.Refresh()
		if err != nil {
			return nil, err
		}
		auth.LastRefresh = time.Now()
		_, err = store.Put(ctx, key, &auth)
		if err != nil {
			return nil, err
		}
	}
	return &auth, nil
}

var (
	downchannels      = make(map[int64]<-chan *avs.Message)
	downchannelsMutex sync.Mutex
)

func downchannelThread(auth ServiceAuth, directives <-chan *avs.Message) {
	log.Printf(". Started downchannel for %d", auth.AccountId)
	for directive := range directives {
		handleDirective(auth, directive, nil)
	}
	log.Printf("! Downchannel for %d closed", auth.AccountId)
	downchannelsMutex.Lock()
	defer downchannelsMutex.Unlock()
	if downchannels[auth.AccountId] == directives {
		delete(downchannels, auth.AccountId)
	}
}

func ensureDownchannel(auth ServiceAuth) {
	downchannelsMutex.Lock()
	defer downchannelsMutex.Unlock()
	if _, ok := downchannels[auth.AccountId]; ok {
		// There's already an active downchannel.
		return
	}
	ch, err := avs.DefaultClient.CreateDownchannel(auth.AccessToken)
	if err != nil {
		log.Printf("! Failed to create downstream for %d: %v", auth.AccountId, err)
		return
	}
	downchannels[auth.AccountId] = ch
	go downchannelThread(auth, ch)
}

type Authorizer interface {
	AccessToken() string
}

type Device struct {
	client        *avs.Client
	auth          Authorizer
	AlertsState   *avs.AlertsState
	PlaybackState *avs.PlaybackState
	SpeechState   *avs.SpeechState
	VolumeState   *avs.VolumeState
	directives    chan avs.TypedMessage
	mx            sync.Mutex
}

func (d *Device) Context() []avs.TypedMessage {
	return []avs.TypedMessage{
		d.AlertsState,
		d.PlaybackState,
		d.SpeechState,
		d.VolumeState,
	}
}

func (d *Device) Running() bool {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.directives != nil
}

func (d *Device) Start() (<-chan avs.TypedMessage, error) {
	d.mx.Lock()
	defer d.mx.Unlock()
	if d.directives != nil {
		return nil, fmt.Errorf("device already started")
	}
	ch, err := d.client.CreateDownchannel(d.auth.AccessToken())
	if err != nil {
		return nil, err
	}
	d.directives = make(chan avs.TypedMessage)
	go d.background(ch)
	return d.directives, nil
}

func (d *Device) Stop() error {
	d.mx.Lock()
	defer d.mx.Unlock()
	if d.directives == nil {
		return fmt.Errorf("device already stopped")
	}
	close(d.directives)
	d.directives = nil
	return nil
}

func (d *Device) Synchronize() error {
	return d.postEvent(avs.NewSynchronizeState(avs.RandomUUIDString()))
}

func (d *Device) background(ch <-chan *avs.Message) {
	for msg := range ch {
		if ok := d.handleDirective(msg.Typed()); !ok {
			break
		}
	}
	d.Stop()
}

func (d *Device) handleDirective(directive avs.TypedMessage) bool {
	d.mx.Lock()
	defer d.mx.Unlock()
	if d.directives == nil {
		return false
	}
	d.directives <- directive
	return true
}

func (d *Device) postEvent(event avs.TypedMessage) error {
	req := avs.NewRequest(d.auth.AccessToken())
	req.Context = d.Context()
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	for _, msg := range resp.Directives {
		if ok := d.handleDirective(msg.Typed()); !ok {
			return fmt.Errorf("device was stopped")
		}
	}
	return nil
}

func NewDevice(client *avs.Client, auth Authorizer) *Device {
	return &Device{
		client: client, auth: auth,
		AlertsState:   avs.NewAlertsState([]avs.Alert{}, []avs.Alert{}),
		PlaybackState: avs.NewPlaybackState("", 0, avs.PlayerActivityIdle),
		SpeechState:   avs.NewSpeechState("", 0, avs.PlayerActivityIdle),
		VolumeState:   avs.NewVolumeState(100, false),
	}
}

func handleDirective(auth ServiceAuth, directive *avs.Message, resp *avs.Response) {
	log.Printf("< Directive for %d: %s", auth.AccountId, directive)
	log.Println(string(directive.GetMessage().Payload))
	switch d := directive.Typed().(type) {
	case *avs.ExpectSpeech:
		log.Printf(". Alexa wants you to speak within %s!", d.Timeout())
	case *avs.Play:
		if cid := d.Payload.AudioItem.Stream.ContentId(); cid != "" {
			save(resp, cid)
		} else {
			log.Printf(". Remote stream: %s", d.Payload.AudioItem.Stream.URL)
		}
		token := avs.RandomUUIDString()
		a, b := avs.PostEvent(auth.AccessToken, avs.NewPlaybackStarted(avs.RandomUUIDString(), token, 0))
		fmt.Println(a, b)
		a, b = avs.PostEvent(auth.AccessToken, avs.NewPlaybackNearlyFinished(avs.RandomUUIDString(), token, 1*time.Second))
		fmt.Println(a, b)
	case *avs.Speak:
		save(resp, d.ContentId())
	case *avs.Message:
		log.Printf("! No typed message for %s\n%v\n%s", d, d.Header, string(d.Payload))
	default:
		log.Printf(". Unhandled directive %s", d)
	}
}

func requestHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("! Failed to read body: %v", err)
		return
	}
	_ = body
	audio, err := os.Open(r.URL.Query().Get("file"))
	if err != nil {
		log.Printf("! Failed to open audio file: %v", err)
		return
	}
	// Get the access token for Amazon.
	beforeAuth := time.Now()
	auth, err := GetAuth(ctx, 5676073085829120)
	if err != nil {
		log.Printf("! Failed to get Alexa auth info: %v", err)
		return
	}
	// Set up a downchannel.
	go ensureDownchannel(*auth)
	// Post the request to AVS.
	beforeRequest := time.Now()
	response, err := avs.PostRecognize(auth.AccessToken, avs.RandomUUIDString(), avs.RandomUUIDString(), audio)
	if err != nil {
		log.Printf("! Failed to call AVS: %v", err)
		return
	}
	if response == nil {
		log.Println(". Alexa had nothing to say.")
		return
	}
	for _, directive := range response.Directives {
		handleDirective(*auth, directive, response)
	}
	log.Printf(". [Fetch auth: %s] [AVS call: %s]", beforeRequest.Sub(beforeAuth), time.Since(beforeRequest))
}

var savedFiles = 0

func save(resp *avs.Response, cid string) {
	savedFiles++
	filename := fmt.Sprintf("./response%d.mp3", savedFiles)
	ioutil.WriteFile(filename, resp.Content[cid], 0666)
	log.Printf(". Saved Alexa’s response to %s", filename)
}
