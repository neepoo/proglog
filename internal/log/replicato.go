package log

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api "github.com/neepoo/proglog/api/v1"
)

type Replicator struct {
	// The replicator connects to other servers with the gRPC client, and we
	// need to configure the client so it can authenticate with the servers.
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	logger  *zap.Logger
	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

// Join method adds the given server address to the
// list of servers to replicate and kicks off the add goroutine to run the
// actual replication logic.
func (r *Replicator) Join(name, addr string) error {
	// The replicator calls the produce function to save a
	//	copy of the messages it consumes from the other servers.
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// already replicating so skip
		return nil
	}
	r.servers[name] = make(chan struct{})

	// 从 addr从头复制数据
	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)
	ctx := context.Background()
	// 节点新加入集群时,从头开始复制数据
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	for {
		/*
			 We replicate
			messages from the other server until that server fails or leaves the
			cluster and the replicator closes the channel for that server
		*/
		select {
		// todo 谁来关闭它
		/*
				Close
			 closes the replicator so it doesn’t replicate new servers that join
			the cluster and it stops replicating existing servers by causing the
			replicate goroutines to return.

		*/
		case <-r.close:
			return
		// todo what's this?
		case <-leave:
			return
		case records := <-records:
			_, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: records})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) logError(err error, msg string, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)

}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

/*
Close
 closes the replicator so it doesn’t replicate new servers that join
the cluster and it stops replicating existing servers by causing the
replicate goroutines to return.

*/
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}
