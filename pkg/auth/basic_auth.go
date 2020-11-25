// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package auth

import (
	"net/http"
	"strings"
)

type BasicAuthMiddleware struct {
	Enable   bool
	Username string
	Password string
}

func (m *BasicAuthMiddleware) HTTPMiddleware(name string, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.Enable {
			next.ServeHTTP(w, r)
			return
		}

		if u, p, ok := r.BasicAuth(); ok && m.Password == p && m.Username == u {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, http.StatusText(401), http.StatusUnauthorized)
	}
}

func NewBasicAuthMiddleware(user, pass string) *BasicAuthMiddleware {
	enable := false
	if user != "" && pass != "" {
		enable = true
	}

	return &BasicAuthMiddleware{
		Enable:   enable,
		Username: strings.TrimSpace(user),
		Password: strings.TrimSpace(pass),
	}
}
