package server

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	api "github.com/neepoo/proglog/api/v1"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type GetServerer interface {
	GetServers() ([]*api.Server, error)
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
	GetServer  GetServerer
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	// Then we add a “grpc.time_ns” field to our structured
	// logs to log the duration of each request in nanoseconds.
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64("grpc.time_ns",
					duration.Nanoseconds(),
				)
			}),
	}
	//  configure how OpenCensus collects metrics and traces:
	trace.ApplyConfig(trace.Config{
		DefaultSampler: trace.AlwaysSample(),
	})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}
	/*
		These lines configure gRPC to apply the Zap interceptors that log the
		gRPC calls and attach OpenCensus as the server’s stat handler so that
		OpenCensus can record stats on the server’s request handling.
	*/
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(), // 设置请求标签
			grpc_zap.StreamServerInterceptor(logger, zapOpts...),
			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_ctxtags.UnaryServerInterceptor(), // 设置请求标签
		grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
		grpc_auth.UnaryServerInterceptor(authenticate),
	)), grpc.StatsHandler(&ocgrpc.ServerHandler{}))
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

type subjectContextKey struct{}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

// grpc 拦截器, 读取证书进行权限认证
func authenticate(ctx context.Context) (context.Context, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if pr.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := pr.AuthInfo.(credentials.TLSInfo)
	// todo look on tlsInfo
	sj := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, sj)
	return ctx, nil

}

func (s grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	// 先进行权限认证
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream 双向流
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		// todo should check io.EOF

		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream 服务端流,从一个制定的偏移量持续消费数据
func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			// When the server reaches the end of the log, the server will wait until someone
			// appends a record to the log and then continue streaming records to the
			// client.
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func (s *grpcServer) GetServers(ctx context.Context, req *api.GetServersRequest) (*api.GetServersResponse, error) {
	servers, err := s.GetServer.GetServers()
	if err != nil {
		return nil, err
	}
	return &api.GetServersResponse{
		Servers: servers,
	}, nil
}
