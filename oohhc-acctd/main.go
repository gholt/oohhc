package main

import (
	"fmt"
	"os"
	"path"

	"golang.org/x/net/context"

	mb "github.com/letterj/oohhc/proto/account"

	"github.com/pandemicsyn/ftls"
	"github.com/pandemicsyn/oort/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"net"
)

// FatalIf is just a lazy log/panic on error func
func FatalIf(err error, msg string) {
	if err != nil {
		grpclog.Fatalf("%s: %v", msg, err)
	}
}

// main is the real deal
func main() {

	cfg := resolveConfig(nil)

	var opts []grpc.ServerOption
	creds, err := credentials.NewServerTLSFromFile(path.Join(cfg.path, "server.crt"), path.Join(cfg.path, "server.key"))
	FatalIf(err, "Couldn't load cert from file")
	opts = []grpc.ServerOption{grpc.Creds(creds)}
	s := grpc.NewServer(opts...)
	ringDialOpts, err := ftls.NewGRPCClientDialOpt(&ftls.Config{
		MutualTLS:          false,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		CAFile:             path.Join(cfg.path, "ca.pem"),
	})
	if err != nil {
		grpclog.Fatalln("Cannot setup tls config for synd client:", err)
	}
	oortDialOpts, err := ftls.NewGRPCClientDialOpt(&ftls.Config{
		MutualTLS:          !cfg.skipMutualTLS,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		CertFile:           path.Join(cfg.path, "client.crt"),
		KeyFile:            path.Join(cfg.path, "client.key"),
		CAFile:             path.Join(cfg.path, "ca.pem"),
	})
	if err != nil {
		grpclog.Fatalln("Cannot setup tls config for oort client:", err)
	}

	clientID, _ := os.Hostname()
	if clientID != "" {
		clientID += "/oohhc-acctd"
	}
	gstore := api.NewReplGroupStore(&api.ReplGroupStoreConfig{
		AddressIndex:       2,
		GRPCOpts:           []grpc.DialOption{oortDialOpts},
		RingServer:         cfg.oortGroupSyndicate,
		RingCachePath:      path.Join(cfg.path, "ring/groupstore.ring"),
		RingServerGRPCOpts: []grpc.DialOption{ringDialOpts},
		RingClientID:       clientID,
	})
	if gerr := gstore.Startup(context.Background()); gerr != nil {
		grpclog.Fatalln("Cannot start valuestore connector:", gerr)
	}

	ws, err := NewAccountWS(cfg.superUserKey, gstore)
	if err != nil {
		grpclog.Fatalln(err)
	}
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port))
	FatalIf(err, "Failed to bind to port")
	mb.RegisterAccountApiServer(s, NewAccountAPIServer(ws))
	grpclog.Printf("Starting up on %d...\n", cfg.port)
	s.Serve(l)
}
