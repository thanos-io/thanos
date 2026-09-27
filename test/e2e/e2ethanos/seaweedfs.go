// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore/providers/s3"
)

const (
	seaweedFSImage     = "chrislusf/seaweedfs:4.47"
	seaweedFSPort      = 8333
	seaweedFSPortName  = "http"
	seaweedFSAdminPort = 23646
	seaweedFSAdminName = "admin"
	SeaweedFSAccessKey = "Cheescake"
	SeaweedFSSecretKey = "supersecret"
)

type SeaweedFSOption func(*seaweedFSOptions)

type seaweedFSOptions struct {
	image     string
	enableTLS bool
}

func WithSeaweedFSImage(image string) SeaweedFSOption {
	return func(o *seaweedFSOptions) {
		o.image = image
	}
}

func WithSeaweedFSTLS() SeaweedFSOption {
	return func(o *seaweedFSOptions) {
		o.enableTLS = true
	}
}

// NewSeaweedFS returns a single-node SeaweedFS server exposing an S3-compatible endpoint.
func NewSeaweedFS(env e2e.Environment, name, bucket string, opts ...SeaweedFSOption) *e2emon.InstrumentedRunnable {
	o := seaweedFSOptions{image: seaweedFSImage}
	for _, opt := range opts {
		opt(&o)
	}

	f := env.Runnable(name).WithPorts(map[string]int{
		seaweedFSPortName:  seaweedFSPort,
		seaweedFSAdminName: seaweedFSAdminPort,
	}).Future()
	dataDir := filepath.Join(f.Dir(), "data")
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create SeaweedFS data directory"))}
	}

	args := []string{
		"mini",
		"-dir=/data",
		fmt.Sprintf("-s3.port=%d", seaweedFSPort),
		"-webdav=false",
		"-s3.port.iceberg=0",
		"-s3.port.lance=0",
		"-s3.autoCreateBucket=false",
	}
	instrumentedOpts := []e2emon.InstrumentedOption{}
	certFile := ""
	keyFile := ""
	caFile := ""
	var readiness e2e.ReadinessProbe = e2e.NewHTTPReadinessProbe(seaweedFSPortName, "/readyz", http.StatusOK, http.StatusOK)

	if o.enableTLS {
		certDir := filepath.Join(f.Dir(), "certs")
		caDir := filepath.Join(certDir, "CAs")
		if err := os.MkdirAll(caDir, 0750); err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create SeaweedFS certificate directory"))}
		}

		certFile = filepath.Join(certDir, "public.crt")
		keyFile = filepath.Join(certDir, "private.key")
		caFile = filepath.Join(caDir, "ca.crt")
		if err := generateSeaweedFSCertificates(certFile, keyFile, caFile, fmt.Sprintf("%s-%s", env.Name(), name)); err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "generate SeaweedFS certificates"))}
		}

		args = append(args,
			"-s3.cert.file="+certFile,
			"-s3.key.file="+keyFile,
		)
		instrumentedOpts = append(instrumentedOpts, e2emon.WithInstrumentedScheme("https"))
		readiness = e2e.NewHTTPSReadinessProbe(seaweedFSPortName, "/readyz", http.StatusOK, http.StatusOK)
	}

	envVars := map[string]string{
		"AWS_ACCESS_KEY_ID":     SeaweedFSAccessKey,
		"AWS_SECRET_ACCESS_KEY": SeaweedFSSecretKey,
	}
	if bucket != "" {
		envVars["S3_BUCKET"] = bucket
		config := s3.DefaultConfig
		config.Bucket = bucket
		config.AccessKey = SeaweedFSAccessKey
		config.SecretKey = SeaweedFSSecretKey
		config.Insecure = !o.enableTLS
		config.BucketLookupType = s3.AutoLookup
		config.HTTPConfig.TLSConfig.CAFile = caFile
		readiness = &seaweedFSBucketReadinessProbe{readiness: readiness, config: config}
	}

	r := f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     o.image,
		Command:   e2e.NewCommand(args[0], args[1:]...),
		EnvVars:   envVars,
		Readiness: readiness,
		Volumes:   []string{dataDir + ":/data:z"},
	}))

	return e2emon.AsInstrumented(r, seaweedFSPortName, instrumentedOpts...)
}

type seaweedFSBucketReadinessProbe struct {
	readiness e2e.ReadinessProbe
	config    s3.Config
}

func (p *seaweedFSBucketReadinessProbe) Ready(runnable e2e.Runnable) error {
	if err := p.readiness.Ready(runnable); err != nil {
		return err
	}

	config := p.config
	config.Endpoint = runnable.Endpoint(seaweedFSPortName)
	bkt, err := s3.NewBucketWithConfig(log.NewNopLogger(), config, "seaweedfs-readiness", nil)
	if err != nil {
		return errors.Wrap(err, "create SeaweedFS readiness bucket")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := bkt.Iter(ctx, "", func(string) error { return nil }); err != nil {
		return errors.Wrap(err, "check SeaweedFS bucket")
	}
	return nil
}

func generateSeaweedFSCertificates(certPath, keyPath, caPath, serverName string) error {
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2019),
		Subject:               pkix.Name{CommonName: "Thanos e2e SeaweedFS CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1658),
		Subject:      pkix.Name{CommonName: serverName},
		DNSNames:     []string{serverName, "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(10, 0, 0),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return errors.Wrap(err, "generate CA key")
	}
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return errors.Wrap(err, "generate server key")
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return errors.Wrap(err, "create CA certificate")
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		return errors.Wrap(err, "create server certificate")
	}

	if err := writePEMFile(caPath, "CERTIFICATE", caDER, 0644); err != nil {
		return errors.Wrap(err, "write CA certificate")
	}
	if err := writePEMFile(certPath, "CERTIFICATE", serverDER, 0644); err != nil {
		return errors.Wrap(err, "write server certificate")
	}
	if err := writePEMFile(keyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(serverKey), 0600); err != nil {
		return errors.Wrap(err, "write server key")
	}
	return nil
}

func writePEMFile(path, blockType string, der []byte, mode os.FileMode) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), mode)
}
