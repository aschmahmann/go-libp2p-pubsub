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
	bootstrapped map[string]chan struct{}

	bootstrappedMux sync.RWMutex

	// discoverQ handles continuing peer discovery
	discoverQ chan *discoverReq

	// ongoing tracks ongoing discovery requests
	ongoing map[string]struct{}

	// done handles completion of a discovery request
	done chan string

	// peerNotifier processes all peer notifications
	peerNotifier chan peer.ID

	// tracks the channels subscribed to new peer notifications
	newPeerNotif map[chan peer.ID]*peerEventTracker

	// setup new peer notifier
	addPeerNotif chan *reqNewPeerNotifier

	// remove peer notifier
	rmPeerNotif chan chan peer.ID
}

// newValidation creates a new validation pipeline
func newDiscover() *discover {
	return &discover{
		advertising:  make(map[string]context.CancelFunc),
		bootstrapped: make(map[string]chan struct{}),
		discoverQ:    make(chan *discoverReq, 32),
		ongoing:      make(map[string]struct{}),
		done:         make(chan string),
		peerNotifier: make(chan peer.ID, 32),
		newPeerNotif: make(map[chan peer.ID]*peerEventTracker),
		addPeerNotif: make(chan *reqNewPeerNotifier),
		rmPeerNotif:  make(chan chan peer.ID),
	}
}

// Start attaches the discovery pipeline to a pubsub instance and starts event loop
func (d *discover) Start(p *PubSub) {
	if d.discovery == nil {
		return
	}

	d.p = p
	go d.discoverLoop()
}

func (d *discover) discoverLoop() {
	for {
		select {
		case discover := <-d.discoverQ:
			topic := discover.topic

			d.bootstrappedMux.RLock()
			bootstrapDone := d.bootstrapped[topic]
			d.bootstrappedMux.RUnlock()
			select {
			case _ = <-bootstrapDone:
			default:
				continue
			}

			if _, ok := d.ongoing[topic]; !ok {
				d.ongoing[topic] = struct{}{}
				go func() {
					d.handleDiscovery(d.p.ctx, topic, discover.opts)
					d.done <- topic
				}()
			}
		case topic := <-d.done:
			delete(d.ongoing, topic)
		case add := <-d.addPeerNotif:
			ch := make(chan peer.ID, 10)
			sentPeers := &peerEventTracker{peers: make(map[peer.ID]struct{})}
			d.newPeerNotif[ch] = sentPeers
			add.resp <- ch
			go func() {
				topicPeers := d.p.ListPeers(add.topic)
				sentPeers.mux.RLock()
				sendPeers := peerIntersect(topicPeers, sentPeers.peers)
				sentPeers.mux.RUnlock()

				sentPeers.mux.Lock()
				for _, pid := range sendPeers {
					if _, ok := sentPeers.peers[pid]; !ok {
						sentPeers.peers[pid] = struct{}{}
						ch <- pid
					}
				}
				sentPeers.mux.Unlock()
			}()
		case rm := <-d.rmPeerNotif:
			delete(d.newPeerNotif, rm)
		case pid := <-d.peerNotifier:
			d.notifyNewPeer(pid)
		case <-d.p.ctx.Done():
			return
		}
	}
}

// Advertise advertises this node's interest in a topic to a discovery service
func (d *discover) Advertise(topic string) {
	if d.discovery == nil {
		return
	}

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

// StopAdvertise stops advertising this node's interest in a topic
func (d *discover) StopAdvertise(topic string) {
	if d.discovery == nil {
		return
	}

	if advertiseCancel, ok := d.advertising[topic]; ok {
		advertiseCancel()
		delete(d.advertising, topic)
	}
}

// Bootstrap handles initial bootstrapping of peers for a given topic
func (d *discover) Bootstrap(async bool, topic string, dOpts ...discovery.Option) {
	if d.discovery == nil {
		return
	}

	d.bootstrappedMux.RLock()
	doneCh, bootstrapStarted := d.bootstrapped[topic]
	d.bootstrappedMux.RUnlock()

	if bootstrapStarted {
		if !async {
			_ = <-doneCh
		}
	} else {
		d.bootstrappedMux.Lock()
		doneCh, bootstrapStarted := d.bootstrapped[topic]
		if bootstrapStarted {
			d.bootstrappedMux.Unlock()
			if !async {
				_ = <-doneCh
			}
			return
		}
		ch := make(chan struct{}, 1)
		d.bootstrapped[topic] = ch
		d.bootstrappedMux.Unlock()

		if async {
			go func() {
				d.handleDiscovery(d.p.ctx, topic, dOpts)
				ch <- struct{}{}
				close(ch)
			}()
		} else {
			d.handleDiscovery(d.p.ctx, topic, dOpts)
			ch <- struct{}{}
			close(ch)
		}
	}
}

// Discover searches for additional peers interested in a given topic
func (d *discover) Discover(topic string, opts ...discovery.Option) {
	if d.discovery == nil {
		return
	}

	d.discoverQ <- &discoverReq{topic, opts}
}

func (d *discover) handleDiscovery(ctx context.Context, topic string, opts []discovery.Option) {
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
		req := &reqNewPeerNotifier{topic: topic, resp: make(chan chan peer.ID, 1)}
		d.addPeerNotif <- req
		peerCh := <-req.resp
		defer func() { d.rmPeerNotif <- peerCh }()
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

func (d *discover) NotifyNewPeer(pid peer.ID) {
	if d.discovery == nil {
		return
	}

	select {
	case d.peerNotifier <- pid:
	default:
		log.Error("dropped msg")
	}
}

func (d *discover) notifyNewPeer(pid peer.ID) {
	for ch, tracker := range d.newPeerNotif {
		tracker.mux.Lock()
		if _, ok := tracker.peers[pid]; !ok {
			select {
			case ch <- pid:
				tracker.peers[pid] = struct{}{}
			default:
				log.Error("dropped msg")
			}
		}
		tracker.mux.Unlock()
	}
}

func peerIntersect(peerArr []peer.ID, peerSet map[peer.ID]struct{}) []peer.ID {
	maxSize := len(peerSet)

	if maxSize > len(peerArr) {
		maxSize = len(peerArr)
	}

	peerBuffer := make([]peer.ID, 0, maxSize)
	for _, pid := range peerArr {
		if _, ok := peerSet[pid]; ok {
			peerBuffer = append(peerBuffer, pid)
		}
	}

	return peerBuffer
}

type reqNewPeerNotifier struct {
	topic string
	resp  chan chan peer.ID
}

type peerEventTracker struct {
	mux   sync.RWMutex
	peers map[peer.ID]struct{}
}

type discoverReq struct {
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
