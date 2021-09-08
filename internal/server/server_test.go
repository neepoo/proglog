package server

import (
	"context"
	"github.com/neepoo/proglog/internal/config"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	api "github.com/neepoo/proglog/api/v1"
	"github.com/neepoo/proglog/internal/log"
)

// setupTest(*testing.T, func(*Config))
// is a helper function to set up each test
// case.
func setupTest(t *testing.T, fn func(config *Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	// server
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// client tls. only set rootCa, client use this verifying server certificates
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)

	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	require.NoError(t, err)
	client = api.NewLogClient(cc)

	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: l.Addr().String(),
			Server:        true,
		})

	require.NoError(t, err)
	serverCCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{CommitLog: clog}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCCreds))
	require.NoError(t, err)
	go func() {
		server.Serve(l)
	}()
	return client, cfg, func() {
		// grpc server
		server.Stop()
		// grpc client
		cc.Close()
		// close listener
		l.Close()
	}
}
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"consume past log boundary fails":                     testConsumerPastBoundary,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, cfg)
		})
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Values: []byte("hello world")}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{Record: want},
	)
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Values, consume.Record.Values)
	require.Equal(t, want.Offset, consume.Record.Offset)

}

func testConsumerPastBoundary(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Values: []byte("hello world"),
		},
	})

	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	require.Nilf(t, consume, "consume not nil")
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{
			Values: []byte("first message"),
			Offset: 0,
		}, {
			Values: []byte("second message"),
			Offset: 1,
		},
	}

	stream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	for offset, record := range records {
		err = stream.Send(&api.ProduceRequest{Record: record})
		require.NoError(t, err)
		res, err := stream.Recv()
		require.NoError(t, err)
		if res.Offset != uint64(offset) {
			t.Fatalf(
				"got offset: %d, want: %d",
				res.Offset,
				offset,
			)
		}
	}
	{
		// test consume stream(server stream)
		stream, err := client.ConsumeStream(ctx,
			&api.ConsumeRequest{
				Offset: 0,
			})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{Values: record.Values, Offset: uint64(i)})
		}
	}
}
