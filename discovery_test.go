package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/discovery"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type mockDiscoveryServer struct {
	mx sync.Mutex
	db map[string]map[peer.ID]*discoveryRegistration
}

type discoveryRegistration struct {
	info peer.AddrInfo
	ttl  time.Duration
}

func newDiscoveryServer() *mockDiscoveryServer {
	return &mockDiscoveryServer{
		db: make(map[string]map[peer.ID]*discoveryRegistration),
	}
}

func (s *mockDiscoveryServer) Advertise(ns string, info peer.AddrInfo, ttl time.Duration) (time.Duration, error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers, ok := s.db[ns]
	if !ok {
		peers = make(map[peer.ID]*discoveryRegistration)
		s.db[ns] = peers
	}
	peers[info.ID] = &discoveryRegistration{info, ttl}
	return ttl, nil
}

func (s *mockDiscoveryServer) FindPeers(ns string, limit int) (<-chan peer.AddrInfo, error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers, ok := s.db[ns]
	if !ok || len(peers) == 0 {
		emptyCh := make(chan peer.AddrInfo)
		close(emptyCh)
		return emptyCh, nil
	}

	count := len(peers)
	if count > limit {
		count = limit
	}
	ch := make(chan peer.AddrInfo, count)
	numSent := 0
	for _, reg := range peers {
		if numSent == count {
			break
		}
		numSent++
		ch <- reg.info
	}
	close(ch)

	return ch, nil
}

func (s *mockDiscoveryServer) hasPeerRecord(ns string, pid peer.ID) bool {
	s.mx.Lock()
	defer s.mx.Unlock()

	if peers, ok := s.db[ns]; ok {
		_, ok := peers[pid]
		return ok
	}
	return false
}

type mockDiscoveryClient struct {
	host   host.Host
	server *mockDiscoveryServer
}

func (d *mockDiscoveryClient) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	var options discovery.Options
	err := options.Apply(opts...)
	if err != nil {
		return 0, err
	}

	return d.server.Advertise(ns, *host.InfoFromHost(d.host), options.Ttl)
}

func (d *mockDiscoveryClient) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	var options discovery.Options
	err := options.Apply(opts...)
	if err != nil {
		return nil, err
	}

	return d.server.FindPeers(ns, options.Limit)
}

func TestSimpleDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup Discovery server and pubsub clients
	const numHosts = 20

	server := newDiscoveryServer()
	discOpts := []discovery.Option{discovery.Limit(numHosts), discovery.TTL(1 * time.Minute)}

	hosts := getNetHosts(t, ctx, numHosts)
	psubs := make([]*PubSub, numHosts)
	for i, h := range hosts {
		disc := &mockDiscoveryClient{h, server}
		psubs[i] = getPubsub(ctx, h, WithDiscovery(disc, discOpts...))
	}

	// Subscribe with all but one pubsub instance
	msgs := make([]*Subscription, numHosts)
	for i, ps := range psubs[1:] {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs[i+1] = subch
	}

	// Wait for the advertisements to go through then check that they did
	for {
		server.mx.Lock()
		numPeers := len(server.db["floodsub:foobar"])
		server.mx.Unlock()
		if numPeers == numHosts-1 {
			break
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}

	for i, h := range hosts[1:] {
		if !server.hasPeerRecord("floodsub:foobar", h.ID()) {
			t.Fatalf("Server did not register host %d with ID: %s", i+1, h.ID().Pretty())
		}
	}

	// Try subscribing followed by publishing a single message
	subch, err := psubs[0].Subscribe("foobar")
	if err != nil {
		t.Fatal(err)
	}
	msgs[0] = subch

	msg := []byte("first message")
	psubs[0].Publish("foobar", msg)

	for _, sub := range msgs {
		got, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(sub.err)
		}
		if !bytes.Equal(msg, got.Data) {
			t.Fatal("got wrong message!")
		}
	}

	// Try random peers sending messages and make sure they are received
	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d the flooooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish("foobar", msg)

		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}
	}
}

func TestGossipSubDiscoveryAfterBootstrap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup Discovery server and pubsub clients
	partitionSize := GossipSubDlo - 1
	numHosts := partitionSize * 2
	const ttl = 1 * time.Minute

	server1, server2 := newDiscoveryServer(), newDiscoveryServer()
	discOpts := []discovery.Option{discovery.Limit(numHosts), discovery.TTL(ttl)}

	// Put the pubsub clients into two partitions
	hosts := getNetHosts(t, ctx, numHosts)
	psubs := make([]*PubSub, numHosts)
	for i, h := range hosts {
		s := server1
		if i >= partitionSize {
			s = server2
		}
		disc := &mockDiscoveryClient{h, s}
		psubs[i] = getGossipsub(ctx, h, WithDiscovery(disc, discOpts...))
	}

	msgs := make([]*Subscription, numHosts)
	for i, ps := range psubs {
		subch, err := ps.Subscribe("foobar")
		if err != nil {
			t.Fatal(err)
		}

		msgs[i] = subch
	}

	// Wait for network to finish forming then join the partitions via discovery
	time.Sleep(time.Second * 2)

	for _, ps := range psubs {
		pn := len(ps.ListPeers("foobar"))
		if pn != partitionSize-1 {
			t.Fatal("partitions not properly formed")
		}
	}

	for i := 0; i < partitionSize; i++ {
		server1.Advertise("floodsub:foobar", *host.InfoFromHost(hosts[i+partitionSize]), ttl)
	}

	// wait for the partitions to join
	time.Sleep(time.Second * 2)

	// verify the network is fully connected
	for _, ps := range psubs {
		pn := len(ps.ListPeers("foobar"))
		if pn != numHosts-1 {
			t.Fatal("network not fully connected")
		}
	}

	// test the mesh
	for i := 0; i < 100; i++ {
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := rand.Intn(len(psubs))

		psubs[owner].Publish("foobar", msg)

		for _, sub := range msgs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(sub.err)
			}
			if !bytes.Equal(msg, got.Data) {
				t.Fatal("got wrong message!")
			}
		}
	}
}
