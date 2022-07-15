// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package ca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"time"

	"github.com/thanos-io/thanos/internal/cortex/util/runutil"
)

type CA struct {
	key    *ecdsa.PrivateKey
	cert   *x509.Certificate
	serial *big.Int
}

func New(name string) *CA {
	key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		panic(err)
	}

	return &CA{
		key: key,
		cert: &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject: pkix.Name{
				Organization: []string{name},
			},
			NotBefore: time.Now(),
			NotAfter:  time.Now().Add(time.Hour * 24 * 180),

			KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			BasicConstraintsValid: true,
			IsCA:                  true,
		},
		serial: big.NewInt(2),
	}

}

func writeExclusivePEMFile(path, marker string, mode os.FileMode, data []byte) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, f, "write pem file")

	if err := pem.Encode(f, &pem.Block{Type: marker, Bytes: data}); err != nil {
		return err
	}
	return nil
}

func (ca *CA) WriteCACertificate(path string) error {
	derBytes, err := x509.CreateCertificate(rand.Reader, ca.cert, ca.cert, ca.key.Public(), ca.key)
	if err != nil {
		return err
	}

	return writeExclusivePEMFile(path, "CERTIFICATE", 0644, derBytes)
}

func (ca *CA) WriteCertificate(template *x509.Certificate, certPath string, keyPath string) error {
	key, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return err
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}

	if err := writeExclusivePEMFile(keyPath, "PRIVATE KEY", 0600, keyBytes); err != nil {
		return err
	}

	template.IsCA = false
	template.NotBefore = time.Now()
	if template.NotAfter.IsZero() {
		template.NotAfter = time.Now().Add(time.Hour * 24 * 180)
	}
	template.SerialNumber = ca.serial.Add(ca.serial, big.NewInt(1))

	derBytes, err := x509.CreateCertificate(rand.Reader, template, ca.cert, key.Public(), ca.key)
	if err != nil {
		return err
	}

	return writeExclusivePEMFile(certPath, "CERTIFICATE", 0644, derBytes)
}
