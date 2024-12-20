package middleware

import (
	"net/http"
)

// BasicAuth returns a middleware that checks HTTP Basic Authentication.
// If the username or password do not match the expected credentials,
// a 401 Unauthorized response is returned. Otherwise, the wrapped handler
// is invoked.
func BasicAuth(next http.HandlerFunc, expectedUser, expectedPassword string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != expectedUser || pass != expectedPassword {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
