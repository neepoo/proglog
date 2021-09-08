package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		// 包含证书chain
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		//Server configurations must set one of Certificates,
			tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile,  // server cert
			cfg.KeyFile,  // server private key
		)
		if err != nil {
			return nil, err
		}
	}

	if cfg.CAFile != "" {
		b, err := ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		// CertPool is a set of certificates.
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM(b)
		if !ok {
			return nil, fmt.Errorf("failed to parse root cartificate: %q", cfg.CAFile)
		}
		if cfg.Server {
			// ClientCAs defines the set of root certificate authorities
			// that servers use if required to verify a client certificate
			// by the policy in ClientAuth.
			tlsConfig.ClientCAs = ca
			// ClientAuth determines the server's policy for
			// TLS Client Authentication. The default is NoClientCert.

			// RequireAndVerifyClientCert indicates that a client certificate should be requested
			// during the handshake, and that at least one valid certificate is required
			// to be sent by the client.
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}

	return tlsConfig, nil
}
