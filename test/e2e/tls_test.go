package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	thTLS "github.com/thanos-io/thanos/pkg/tls"
	pb "google.golang.org/grpc/examples/features/proto/echo"
)

var serverName = "thanos"

func TestGRPCServerCertAutoRotate(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	logger := log.NewLogfmtLogger(os.Stderr)
	expMessage := "hello world"

	tmpDirClt, err := ioutil.TempDir("", "test-tls-clt")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDirClt)) }()
	caClt := filepath.Join(tmpDirClt, "ca")
	certClt := filepath.Join(tmpDirClt, "cert")
	keyClt := filepath.Join(tmpDirClt, "key")

	tmpDirSrv, err := ioutil.TempDir("", "test-tls-srv")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDirSrv)) }()
	caSrv := filepath.Join(tmpDirSrv, "ca")
	certSrv := filepath.Join(tmpDirSrv, "cert")
	keySrv := filepath.Join(tmpDirSrv, "key")

	genCerts(t, certSrv, keySrv, caClt, serverName)
	genCerts(t, certClt, keyClt, caSrv, serverName)

	configSrv, err := thTLS.NewServerConfig(logger, certSrv, keySrv, caSrv)
	testutil.Ok(t, err)

	srv := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 1 * time.Millisecond}), grpc.Creds(credentials.NewTLS(configSrv)))

	pb.RegisterEchoServer(srv, &ecServer{})
	p, err := testutil.FreePort()
	testutil.Ok(t, err)
	addr := fmt.Sprint("localhost:", p)
	lis, err := net.Listen("tcp", addr)
	testutil.Ok(t, err)

	go func() {
		testutil.Ok(t, srv.Serve(lis))
	}()
	defer func() { srv.Stop() }()
	time.Sleep(50 * time.Millisecond) // Wait for the server to start.

	// Setup the connection and the client.
	configClt, err := thTLS.NewClientConfig(logger, certClt, keyClt, caClt, serverName)
	testutil.Ok(t, err)
	conn, err := grpc.Dial(addr, grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: 1 * time.Minute}), grpc.WithTransportCredentials(credentials.NewTLS(configClt)))
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, conn.Close())
	}()
	clt := pb.NewEchoClient(conn)

	// Check a good state.
	resp, err := clt.UnaryEcho(context.Background(), &pb.EchoRequest{Message: expMessage})
	testutil.Ok(t, err)
	testutil.Equals(t, expMessage, resp.Message)

	// Reload the server certs and ensure that the tls handshake fails.
	genCerts(t, certSrv, keySrv, "", serverName)
	time.Sleep(50 * time.Millisecond) // Wait for the server MaxConnectionAge to expire.
	_, err = clt.UnaryEcho(context.Background(), &pb.EchoRequest{Message: expMessage})
	checkAuthError(t, err)

	// Restore a good tls state.
	genCerts(t, certSrv, keySrv, caClt, serverName)
	genCerts(t, certClt, keyClt, caSrv, serverName)
	time.Sleep(50 * time.Millisecond) // Wait for the server MaxConnectionAge to expire.
	resp, err = clt.UnaryEcho(context.Background(), &pb.EchoRequest{Message: expMessage})
	testutil.Ok(t, err)
	testutil.Equals(t, expMessage, resp.Message)

	// Reload the client certs and ensure that the tls handshake fails.
	genCerts(t, certClt, keyClt, "", serverName)
	time.Sleep(50 * time.Millisecond) // Wait for the server MaxConnectionAge to expire.
	_, err = clt.UnaryEcho(context.Background(), &pb.EchoRequest{Message: expMessage})
	checkAuthError(t, err)
}

func checkAuthError(t *testing.T, err error) {
	testutil.NotOk(t, err)
	// The first type assertion returns the rpc error.
	err, _ = err.(x509.UnknownAuthorityError)
	// The second type assertion returns the tls error.
	err, isUnknownAuthorityError := err.(x509.UnknownAuthorityError)
	testutil.Assert(t, isUnknownAuthorityError, "expected UnknownAuthorityError, but got:%v", err)
}

var caRoot = &x509.Certificate{
	SerialNumber:          big.NewInt(2019),
	NotAfter:              time.Now().AddDate(10, 0, 0),
	IsCA:                  true,
	ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	BasicConstraintsValid: true,
}

var cert = &x509.Certificate{
	SerialNumber: big.NewInt(1658),
	DNSNames:     []string{serverName},
	NotAfter:     time.Now().AddDate(10, 0, 0),
	SubjectKeyId: []byte{1, 2, 3},
	ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	KeyUsage:     x509.KeyUsageDigitalSignature,
}

// genCerts generates certificates and writes those to the provided paths.
// When the CA file already exists it is not overwritten and
// it is used to sign the certificates.
func genCerts(t *testing.T, certPath, privkeyPath, caPath, serverName string) {
	var (
		err       error
		caPrivKey *rsa.PrivateKey
		caSrvPriv = caPath + ".priv"
	)

	// When the CA private file exists don't overwrite it but
	// use it to extract the private key to be used for signing the certificate.
	if _, err := os.Stat(caSrvPriv); !os.IsNotExist(err) {
		d, err := ioutil.ReadFile(caSrvPriv)
		testutil.Ok(t, err)
		caPrivKey, err = x509.ParsePKCS1PrivateKey(d)
		testutil.Ok(t, err)
	} else {
		caPrivKey, err = rsa.GenerateKey(rand.Reader, 1024)
		testutil.Ok(t, err)
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 1024)
	testutil.Ok(t, err)

	// Sign the cert with the CA private key.
	certBytes, err := x509.CreateCertificate(rand.Reader, cert, caRoot, &certPrivKey.PublicKey, caPrivKey)
	testutil.Ok(t, err)

	if caPath != "" {
		caBytes, err := x509.CreateCertificate(rand.Reader, caRoot, caRoot, &caPrivKey.PublicKey, caPrivKey)
		testutil.Ok(t, err)
		caPEM := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: caBytes,
		})
		testutil.Ok(t, ioutil.WriteFile(caPath, caPEM, 0644))
		testutil.Ok(t, ioutil.WriteFile(caSrvPriv, x509.MarshalPKCS1PrivateKey(caPrivKey), 0644))
	}

	if certPath != "" {
		certPEM := new(bytes.Buffer)
		testutil.Ok(t, pem.Encode(certPEM, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: certBytes,
		}))
		testutil.Ok(t, ioutil.WriteFile(certPath, certPEM.Bytes(), 0644))
	}

	if privkeyPath != "" {
		certPrivKeyPEM := new(bytes.Buffer)
		testutil.Ok(t, pem.Encode(certPrivKeyPEM, &pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
		}))
		testutil.Ok(t, ioutil.WriteFile(privkeyPath, certPrivKeyPEM.Bytes(), 0644))
	}
}

type ecServer struct {
	pb.UnimplementedEchoServer
}

func (s *ecServer) UnaryEcho(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	return &pb.EchoResponse{Message: req.Message}, nil
}
