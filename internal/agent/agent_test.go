package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/neepoo/proglog/api/v1"
	"github.com/neepoo/proglog/internal/agent"
	"github.com/neepoo/proglog/internal/config"
	"github.com/neepoo/proglog/internal/loadbalance"
)

/*We’ll set up a cluster with three nodes. We’ll produce a record
to one server and verify that we can consume the message from the
other servers that have (hopefully) replicated for us.
*/

func TestAgent(t *testing.T) {
	// 用来监听rpc请求
	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: "127.0.0.1",
			Server:        true,
		},
	)
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			ServerAddress: "127.0.0.1",
			Server:        false,
		})
	require.NoError(t, err)

	// 设置集群
	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i > 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		agent, err := agent.New(
			agent.Config{
				ServerTLSConfig: serverTLSConfig,
				PeerTLSConfig:   peerTLSConfig,
				DataDir:         dataDir,
				BindAddr:        bindAddr,
				RPCPort:         rpcPort,
				NodeName:        fmt.Sprintf("node-%d", i),
				StartJoinAddrs:  startJoinAddrs,
				ACLModeFile:     config.ACLModelFile,
				ACLPolicyFile:   config.ACLPolicyFile,
				Bootstrap:       i == 0,
			})
		require.NoError(t, err)
		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()
	// 等待服务之间彼此发现
	time.Sleep(3 * time.Second)
	/*
		Now we check that Raft has replicated the record we produced to the
		leader by consuming the record from a follower and that the replication
		stops there
	*/
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{Record: &api.Record{
			Values: []byte("foo"),
		}},
	)
	require.NoError(t, err)
	// 等待复制完成
	time.Sleep(3 * time.Second)

	consumeResponse, err := leaderClient.Consume(context.Background(), &api.ConsumeRequest{Offset: produceResponse.Offset})
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Values, []byte("foo"))


	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Values, []byte("foo"))
	consumeResponse, err = leaderClient.Consume(context.Background(), &api.ConsumeRequest{
		Offset: produceResponse.Offset + 1,
	})
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)

}

func client(
	t *testing.T,
	agent *agent.Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(
		// specify our scheme in the URL so gRPC knows to
		// use our resolver.
		fmt.Sprintf("%s:///%s", loadbalance.Name, rpcAddr),
		opts...,
	)
	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}
