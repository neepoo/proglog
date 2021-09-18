package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/neepoo/proglog/api/v1"
)

const Name = "proglog"

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Builder = (*Resolver)(nil)

type Resolver struct {
	mu sync.Mutex

	// The clientConn connection is the user’s client
	//	connection and gRPC passes it to the resolver for the resolver to
	//	update with the servers it discovers.
	clientConn resolver.ClientConn

	//  The resolverConn is the resolver’s
	//	own client connection to the server so it can call GetServers and get the
	//	servers.
	resolverConn *grpc.ClientConn

	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

// implement resolver.Resolver

// ResolveNow called by gRPC to resolve the target, discover the servers, and update
//the client connection with the servers.
func (r *Resolver) ResolveNow(options resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error("failed to resolve server", zap.Error(err))
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(
			addrs,
			resolver.Address{
				Addr: server.RpcAddr,
				Attributes: attributes.New(
					"is_leader",
					server.IsLeader,
				),
			},
		)
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})

}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}

// implement resolver.Builder interface

func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(fmt.Sprintf(`{"loadBalanceConfig":[{"%s":{}}]}`, Name))
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (r *Resolver) Scheme() string {
	return Name
}
