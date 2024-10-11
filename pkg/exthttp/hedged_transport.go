package exthttp

import (
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cristalhq/hedgedhttp"
	"github.com/prometheus/common/model"
)

var DefaultHTTPConfig = HTTPConfig{
	IdleConnTimeout:       model.Duration(90 * time.Second),
	ResponseHeaderTimeout: model.Duration(2 * time.Minute),
	TLSHandshakeTimeout:   model.Duration(10 * time.Second),
	ExpectContinueTimeout: model.Duration(1 * time.Second),
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   100,
	MaxConnsPerHost:       0,
}

func HedgedTransport(config HTTPConfig) (http.RoundTripper, error) {
	tlsConfig, err := NewTLSConfig(&config.TLSConfig)
	if err != nil {
		return nil, err
	}
	tlsConfig.InsecureSkipVerify = config.InsecureSkipVerify

	// base RoundTripper
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          config.MaxIdleConns,
		MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(config.IdleConnTimeout),
		MaxConnsPerHost:       config.MaxConnsPerHost,
		TLSHandshakeTimeout:   time.Duration(config.TLSHandshakeTimeout),
		ExpectContinueTimeout: time.Duration(config.ExpectContinueTimeout),
		ResponseHeaderTimeout: time.Duration(config.ResponseHeaderTimeout),
		TLSClientConfig:       tlsConfig,
	}
	// hedged RoundTripper
	delay := 10
	upto := 3
	hedgedTransport, err := hedgedhttp.NewRoundTripper(
		time.Duration(delay), // Timeout for hedged requests
		upto,                 // Maximum number of hedged requests
		transport,            // Base RoundTripper
	)
	if err != nil {
		return nil, err
	}
	return hedgedTransport, nil
}

// LoggingRoundTripper logs the requests and forwards them to the base RoundTripper.
type LoggingRoundTripper struct {
	Transport http.RoundTripper
}

func (lrt *LoggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Log the HTTP method and URL of the request
	log.Printf("Request: %s %s", req.Method, req.URL.String())

	// Proceed with the actual request using the base transport
	resp, err := lrt.Transport.RoundTrip(req)

	if err != nil {
		log.Printf("Request to %s failed: %v", req.URL.String(), err)
	} else {
		log.Printf("Response from %s: %s", req.URL.String(), resp.Status)
	}

	return resp, err
}

func HedgedTransportWithLogging(config HTTPConfig) (http.RoundTripper, error) {
	// Create the original hedged transport
	hedgedTransport, err := HedgedTransport(config)
	if err != nil {
		return nil, err
	}

	// Wrap it with logging RoundTripper
	return &LoggingRoundTripper{Transport: hedgedTransport}, nil
}
