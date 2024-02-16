// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tenancy

import (
	"context"
	"net/http"
	"path"

	"github.com/pkg/errors"
	"github.com/prometheus-community/prom-label-proxy/injectproxy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"google.golang.org/grpc/metadata"
)

type contextKey int

const (
	// DefaultTenantHeader is the default header used to designate the tenant making a request.
	DefaultTenantHeader = "THANOS-TENANT"
	// DefaultTenant is the default value used for when no tenant is passed via the tenant header.
	DefaultTenant = "default-tenant"
	// DefaultTenantLabel is the default label-name with which the tenant is announced in stored metrics.
	DefaultTenantLabel = "tenant_id"
	// This key is used to pass tenant information using Context.
	TenantKey contextKey = 0
	// MetricLabel is the label name used for adding tenant information to exported metrics.
	MetricLabel = "tenant"
)

// Allowed fields in client certificates.
const (
	CertificateFieldOrganization       = "organization"
	CertificateFieldOrganizationalUnit = "organizationalUnit"
	CertificateFieldCommonName         = "commonName"
)

func IsTenantValid(tenant string) error {
	if tenant != path.Base(tenant) {
		return errors.New("Tenant name not valid")
	}
	return nil
}

// GetTenantFromHTTP extracts the tenant from a http.Request object.
func GetTenantFromHTTP(r *http.Request, tenantHeader string, defaultTenantID string, certTenantField string) (string, error) {
	var err error
	tenant := r.Header.Get(tenantHeader)
	if tenant == "" {
		tenant = r.Header.Get(DefaultTenantHeader)
		if tenant == "" {
			tenant = defaultTenantID
		}
	}

	if certTenantField != "" {
		tenant, err = getTenantFromCertificate(r, certTenantField)
		if err != nil {
			// This must hard fail to ensure hard tenancy when feature is enabled.
			return "", err
		}
	}

	err = IsTenantValid(tenant)
	if err != nil {
		return "", err
	}
	return tenant, nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (r roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return r(request)
}

// InternalTenancyConversionTripper is a tripperware that rewrites the configurable tenancy header in the request into
// the hardcoded tenancy header that is used for internal communication in Thanos components. If any custom tenant
// header is configured and present in the request, it will be stripped out.
func InternalTenancyConversionTripper(customTenantHeader, certTenantField string, next http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		tenant, _ := GetTenantFromHTTP(r, customTenantHeader, DefaultTenant, certTenantField)
		r.Header.Set(DefaultTenantHeader, tenant)
		// If the custom tenant header is not the same as the default internal header, we want to exclude the custom
		// one from the request to keep things simple.
		if customTenantHeader != DefaultTenantHeader {
			r.Header.Del(customTenantHeader)
		}
		return next.RoundTrip(r)
	})
}

// getTenantFromCertificate extracts the tenant value from a client's presented certificate. The x509 field to use as
// value can be configured with Options.TenantField. An error is returned when the extraction has not succeeded.
func getTenantFromCertificate(r *http.Request, certTenantField string) (string, error) {
	var tenant string

	if len(r.TLS.PeerCertificates) == 0 {
		return "", errors.New("could not get required certificate field from client cert")
	}

	// First cert is the leaf authenticated against.
	cert := r.TLS.PeerCertificates[0]

	switch certTenantField {

	case CertificateFieldOrganization:
		if len(cert.Subject.Organization) == 0 {
			return "", errors.New("could not get organization field from client cert")
		}
		tenant = cert.Subject.Organization[0]

	case CertificateFieldOrganizationalUnit:
		if len(cert.Subject.OrganizationalUnit) == 0 {
			return "", errors.New("could not get organizationalUnit field from client cert")
		}
		tenant = cert.Subject.OrganizationalUnit[0]

	case CertificateFieldCommonName:
		if cert.Subject.CommonName == "" {
			return "", errors.New("could not get commonName field from client cert")
		}
		tenant = cert.Subject.CommonName

	default:
		return "", errors.New("tls client cert field requested is not supported")
	}

	return tenant, nil
}

func GetTenantFromGRPCMetadata(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get(DefaultTenantHeader)) == 0 {
		return DefaultTenant, false
	}
	return md.Get(DefaultTenantHeader)[0], true
}

func EnforceQueryTenancy(tenantLabel string, tenant string, query string) (string, error) {
	labelMatcher := &labels.Matcher{
		Name:  tenantLabel,
		Type:  labels.MatchEqual,
		Value: tenant,
	}

	e := injectproxy.NewEnforcer(false, labelMatcher)

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", errors.Wrap(err, "error parsing query string, when enforcing tenenacy")
	}

	if err := e.EnforceNode(expr); err != nil {
		return "", errors.Wrap(err, "error enforcing label")
	}

	return expr.String(), nil
}

func getLabelMatchers(formMatchers []string, tenant string, enforceTenancy bool, tenantLabel string) ([][]*labels.Matcher, error) {
	tenantLabelMatcher := &labels.Matcher{
		Name:  tenantLabel,
		Type:  labels.MatchEqual,
		Value: tenant,
	}

	matcherSets := make([][]*labels.Matcher, 0, len(formMatchers))

	// If tenancy is enforced, but there are no matchers at all, add the tenant matcher
	if len(formMatchers) == 0 && enforceTenancy {
		var matcher []*labels.Matcher
		matcher = append(matcher, tenantLabelMatcher)
		matcherSets = append(matcherSets, matcher)
		return matcherSets, nil
	}

	for _, s := range formMatchers {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, err
		}

		if enforceTenancy {
			e := injectproxy.NewEnforcer(false, tenantLabelMatcher)
			matchers, err = e.EnforceMatchers(matchers)
			if err != nil {
				return nil, err
			}
		}

		matcherSets = append(matcherSets, matchers)
	}

	return matcherSets, nil
}

// This function will:
// - Get tenant from HTTP header and add it to context.
// - if tenancy is enforced, add a tenant matcher to the promQL expression.
func RewritePromQL(ctx context.Context, r *http.Request, tenantHeader string, defaultTenantID string, certTenantField string, enforceTenancy bool, tenantLabel string, queryStr string) (string, string, context.Context, error) {
	tenant, err := GetTenantFromHTTP(r, tenantHeader, defaultTenantID, certTenantField)
	if err != nil {
		return "", "", ctx, err
	}
	ctx = context.WithValue(ctx, TenantKey, tenant)

	if enforceTenancy {
		queryStr, err = EnforceQueryTenancy(tenantLabel, tenant, queryStr)
		return queryStr, tenant, ctx, err
	}
	return queryStr, tenant, ctx, nil
}

// This function will:
// - Get tenant from HTTP header and add it to context.
// - Parse all labels matchers provided.
// - If tenancy is enforced, make sure a tenant matcher is present.
func RewriteLabelMatchers(ctx context.Context, r *http.Request, tenantHeader string, defaultTenantID string, certTenantField string, enforceTenancy bool, tenantLabel string, formMatchers []string) ([][]*labels.Matcher, context.Context, error) {
	tenant, err := GetTenantFromHTTP(r, tenantHeader, defaultTenantID, certTenantField)
	if err != nil {
		return nil, ctx, err
	}
	ctx = context.WithValue(ctx, TenantKey, tenant)

	matcherSets, err := getLabelMatchers(formMatchers, tenant, enforceTenancy, tenantLabel)
	if err != nil {
		return nil, ctx, err
	}

	return matcherSets, ctx, nil
}
