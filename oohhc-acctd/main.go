package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	mb "github.com/letterj/oohhc/proto/account"

	"github.com/pandemicsyn/ftls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"net"
)

var (
	usetls             = flag.Bool("tls", true, "Connection uses TLS if true, else plain TCP")
	certFile           = flag.String("cert_file", "/etc/oort/server.crt", "The TLS cert file")
	keyFile            = flag.String("key_file", "/etc/oort/server.key", "The TLS key file")
	port               = flag.Int("port", 8449, "The acctd server port")
	oortGroupHost      = flag.String("oortgrouphost", "127.0.0.1:6380", "host:port to use when connecting to oort group")
	insecureSkipVerify = flag.Bool("skipverify", true, "don't verify cert")
	superUserKey       = flag.String("superkey", "123456789", "Super User key used for authentication")
	// Group Store Values
	mutualtlsGS          = flag.Bool("mutualtlsGS", true, "Turn on MutualTLS for Group Store")
	insecureSkipVerifyGS = flag.Bool("insecureSkipVerifyGS", false, "Don't verify cert for Group Store")
	certFileGS           = flag.String("certfileGS", "/etc/oort/client.crt", "The client TLS cert file for the Group Store")
	keyFileGS            = flag.String("keyFileGS", "/etc/oort/client.key", "The client TLS key file for the Group Store")
	caFileGS             = flag.String("cafileGS", "/etc/oort/ca.pem", "The client CA file")
)

// FatalIf is just a lazy log/panic on error func
func FatalIf(err error, msg string) {
	if err != nil {
		grpclog.Fatalf("%s: %v", msg, err)
	}
}

func main() {
	flag.Parse()

	envMutualtlsGS := os.Getenv("OOHHC_GS_MUTUALTLS")
	if envMutualtlsGS == "false" {
		*mutualtlsGS = false
	}

	envInsecureSkipVerifyGS := os.Getenv("OOHHC_GS_SKIP_VERIFY")
	if envInsecureSkipVerifyGS == "false" {
		*insecureSkipVerify = false
	}

	envCertFileGS := os.Getenv("OOHHC_GS_CERT_FILE")
	if envCertFileGS != "" {
		*certFileGS = envCertFileGS
	}

	envKeyFileGS := os.Getenv("OOHHC_GS_KEY_FILE")
	if envKeyFileGS != "" {
		*keyFileGS = envKeyFileGS
	}

	envCAFileGS := os.Getenv("OOHHC_GS_CA_FILE")
	if envCAFileGS != "" {
		*caFileGS = envCAFileGS
	}

	envtls := os.Getenv("OOHHC_ACCT_TLS")
	if envtls == "false" {
		*usetls = false
	}

	envoortghost := os.Getenv("OOHHC_OORT_GROUP_HOST")
	if envoortghost != "" {
		*oortGroupHost = envoortghost
	}

	envport := os.Getenv("OOHHC_ACCT_PORT")
	if envport != "" {
		p, err := strconv.Atoi(envport)
		if err != nil {
			log.Println("Did not send valid port from env:", err)
		} else {
			*port = p
		}
	}

	envcert := os.Getenv("OOHHC_ACCT_CERT_FILE")
	if envcert != "" {
		*certFile = envcert
	}

	envkey := os.Getenv("OOHHC_ACCT_KEY_FILE")
	if envkey != "" {
		*keyFile = envkey
	}

	envSuperKey := os.Getenv("OOHHC_ACCT_SUPERUSER_KEY")
	if envSuperKey != "" {
		*superUserKey = envSuperKey
	}

	envSkipVerify := os.Getenv("OOHHC_ACCT_SKIP_VERIFY")
	if envSkipVerify != "true" {
		*insecureSkipVerify = true
	}

	gopt, err := ftls.NewGRPCClientDialOpt(&ftls.Config{
		MutualTLS:          *mutualtlsGS,
		InsecureSkipVerify: *insecureSkipVerifyGS,
		CertFile:           *certFileGS,
		KeyFile:            *keyFileGS,
		CAFile:             *caFileGS,
	})
	if err != nil {
		grpclog.Fatalln("Cannot setup tls config:", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	FatalIf(err, "Failed to bind to port")

	var opts []grpc.ServerOption
	if *usetls {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		FatalIf(err, "Couldn't load cert from file")
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	s := grpc.NewServer(opts...)
	ws, err := NewAccountWS(*superUserKey, *oortGroupHost, *insecureSkipVerify, gopt)
	if err != nil {
		grpclog.Fatalln(err)
	}
	mb.RegisterAccountApiServer(s, NewAccountAPIServer(ws))
	grpclog.Printf("Starting up on %d...\n", *port)
	s.Serve(lis)
}
