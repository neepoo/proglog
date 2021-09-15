package server

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/neepoo/proglog/api/v1"
	"github.com/neepoo/proglog/internal/auth"
	"github.com/neepoo/proglog/internal/config"
	"github.com/neepoo/proglog/internal/log"
)

var (
	debug             = flag.Bool("debug", false, "Enable observability for debugging.")
	telemetryExporter *exporter.LogExporter
)

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

// setupTest(*testing.T, func(*Config))
// is a helper function to set up each test
// case.
func setupTest(t *testing.T, fn func(config *Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	// server
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// let’s update our rootClient setup in our tests to build two clients we
	// can use for testing our authorization setup.

	newClint := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(
			config.TLSConfig{
				CertFile: crtPath,
				KeyFile:  keyPath,
				CAFile:   config.CAFile,
				Server:   false,
			},
		)
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClint(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn

	nobodyConn, nobodyClient, _ = newClint(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

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
	defer os.RemoveAll(dir)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{CommitLog: clog, Authorizer: authorizer}
	if fn != nil {
		fn(cfg)
	}

	// observer
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := ioutil.TempFile("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			ReportingInterval: time.Second,
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCCreds))
	require.NoError(t, err)
	go func() {
		server.Serve(l)
	}()
	return rootClient, nobodyClient, cfg, func() {
		// grpc server
		server.Stop()
		// grpc rootClient
		rootConn.Close()
		nobodyConn.Close()
		// close listener
		l.Close()
		if telemetryExporter != nil {
			// We sleep for 1.5 seconds to give the telemetry exporter enough time to
			//flush its data to disk. Then we stop and close the exporter.
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"consume past log boundary fails":                     testConsumerPastBoundary,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"unauthorized fails":                                  testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, cfg)
		})
	}
}

func testProduceConsume(
	t *testing.T,
	client, _ api.LogClient,
	cfg *Config,
) {
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
	client, _ api.LogClient,
	cfg *Config,
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
	client, _ api.LogClient,
	cfg *Config,
) {
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
func testUnauthorized(
	t *testing.T,
	_,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produce, err := client.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Values: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}

//
//func testUnauthorized(
//	t *testing.T,
//	_,
//	client api.LogClient,
//	config *Config,
//) {
//	ctx := context.Background()
//
//	producer, err := client.Produce(ctx,
//		&api.ProduceRequest{Record: &api.Record{
//			Values: []byte("I'm bat man!"),
//		}})
//	require.Nil(t, producer, "produce response should be nil")
//	gotCode, wantCode := status.Code(err), codes.PermissionDenied
//	if gotCode != wantCode {
//		t.Fatalf("got code: %s, want: %d", gotCode, wantCode)
//	}
//	consume, err := client.Consume(ctx, &api.ConsumeRequest{
//		Offset: 0,
//	})
//	require.Nil(t, consume, "consume response should be nil")
//	gotCode, wantCode = status.Code(err), codes.PermissionDenied
//	if gotCode != wantCode {
//		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
//	}
//}
