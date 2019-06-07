package pubsub

import (
	"context"
	"github.com/libp2p/go-libp2p-core/discovery"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
	"time"
)

// discover represents the discovery pipeline.
// The discovery pipeline handles advertising and discovery of peers
type discover struct {
	p *PubSub

	// discovery assists in discovering and advertising peers for a topic
	discovery discovery.Discovery

	// advertising tracks which topics are being advertised
	advertising map[string]context.CancelFunc

	// bootstrapped tracks which topics have been bootstrapped
	bootstrapped sync.Map

	// discover handles continuing peer discovery
	discover chan *Discover

	// ongoingDiscovery tracks ongoing discovery requests
	ongoingDiscovery map[string]struct{}

	// doneDiscovery handles completion of a discovery request
	doneDiscovery chan string
}

// newValidation creates a new validation pipeline
func newDiscover() *discover {
	return &discover{
		bootstrapped:     sync.Map{},
		discover:         make(chan *Discover),
		ongoingDiscovery: make(map[string]struct{}),
		doneDiscovery:    make(chan string),
		advertising:      make(map[string]context.CancelFunc),
	}
}

// Start attaches the discovery pipeline to a pubsub instance and starts event loop
func (d *discover) Start(p *PubSub) {
	d.p = p
	go d.discoverLoop()
}

func (d *discover) discoverLoop() {
	for {
		select {
		case discover := <-d.discover:
			topic := discover.topic
			if _, ok := d.ongoingDiscovery[topic]; !ok {
				d.ongoingDiscovery[topic] = struct{}{}
				go func() {
					d.handleDiscovery(d.p.ctx, topic, discover.opts)
					d.doneDiscovery <- topic
				}()
			}
		case topic := <-d.doneDiscovery:
			delete(d.ongoingDiscovery, topic)
		case <-d.p.ctx.Done():
			log.Info("pubsub discoverloop shutting down")
			return
		}
	}
}

// advertise advertises this node's interest in a topic to a discovery service
func (d *discover) advertise(topic string) {
	if d.discovery != nil {
		advertisingCtx, cancel := context.WithCancel(d.p.ctx)

		if _, ok := d.advertising[topic]; ok {
			return
		}
		d.advertising[topic] = cancel

		go func() {
			next, err := d.discovery.Advertise(advertisingCtx, topic)
			if err != nil {
				log.Warningf("bootstrap: error providing rendezvous for %s: %s", topic, err.Error())
			}

			t := time.NewTimer(next)
			for {
				select {
				case <-t.C:
					next, err = d.discovery.Advertise(advertisingCtx, topic)
					if err != nil {
						log.Warningf("bootstrap: error providing rendezvous for %s: %s", topic, err.Error())
					}
					t.Reset(next)
				case <-advertisingCtx.Done():
					t.Stop()
					return
				}
			}
		}()
	}
}

func (d *discover) stopAdvertise(topic string) {
	if advertiseCancel, ok := d.advertising[topic]; ok {
		advertiseCancel()
		delete(d.advertising, topic)
	}
}

// bootstrap handles initial bootstrapping of peers for a given topic
func (d *discover) bootstrap(async bool, topic string, dOpts ...discovery.Option) {
	newMux := &sync.Mutex{}
	newMux.Lock()
	bootstrapMux, bootstrapStarted := d.bootstrapped.LoadOrStore(topic, newMux)

	if bootstrapStarted {
		newMux.Unlock()
		if async {
			return
		}
		mux := bootstrapMux.(*sync.Mutex)
		mux.Lock()
		mux.Unlock()
	} else {
		d.handleDiscovery(d.p.ctx, topic, dOpts)
		newMux.Unlock()
	}
}

func (d *discover) handleDiscovery(ctx context.Context, topic string, opts []discovery.Option) {
	if d.discovery == nil {
		return
	}

	discoverCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	peerChan, err := d.discovery.FindPeers(discoverCtx, topic, opts...)
	if err != nil {
		log.Debugf("error finding peers for topic %s: %v", topic, err)
		return
	}

	findPeers := make(map[peer.ID]struct{})

loop:
	for {
		select {
		case pi, more := <-peerChan:
			if pi.ID == d.p.host.ID() || pi.ID == "" {
				if more {
					continue
				} else {
					break loop
				}
			}

			findPeers[pi.ID] = struct{}{}

			go func(pi peer.AddrInfo) {
				ctx, cancel := context.WithTimeout(ctx, time.Second*10)
				defer cancel()

				err := d.p.host.Connect(ctx, pi)
				if err != nil {
					log.Debugf("Error connecting to pubsub peer %s: %s", pi.ID, err.Error())
					return
				}
			}(pi)

			if !more {
				break loop
			}

		case <-discoverCtx.Done():
			log.Infof("pubsub discovery for %s timeout", topic)
			return
		}
	}

	if len(findPeers) > 0 {
		req := &reqNewPeerNotifier{initPeers: findPeers, resp: make(chan chan peer.ID, 1)}
		d.p.addPeerNotif <- req
		peerCh := <-req.resp
		defer func() { d.p.rmPeerNotif <- peerCh }()
		for {
			select {
			case pid := <-peerCh:
				if _, ok := findPeers[pid]; ok {
					delete(findPeers, pid)
					log.Debugf("Connected to pubsub peer %s", pid)
					if len(findPeers) == 0 {
						return
					}
				}
			case <-discoverCtx.Done():
				log.Infof("pubsub discovery for %s timeout", topic)
				return
			}
		}
	}
}

type Discover struct {
	topic string
	opts  []discovery.Option
}

type pubSubDiscovery struct {
	discovery.Discovery
	opts []discovery.Option
}

// Advertise advertises a service
func (d *pubSubDiscovery) Advertise(ctx context.Context, ns string, opts ...discovery.Option) (time.Duration, error) {
	return d.Discovery.Advertise(ctx, "floodsub:"+ns, append(opts, d.opts...)...)
}

func (d *pubSubDiscovery) FindPeers(ctx context.Context, ns string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	return d.Discovery.FindPeers(ctx, "floodsub:"+ns, append(opts, d.opts...)...)
}
