/*
Copyright 2009-2016 Weibo, Inc.

All files licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
)

const basicAuthPrefix string = "Basic "

func CompatibleWarp(handle http.HandlerFunc) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		handle(w, req)
	}
}

func BasicAuthWarp(h httprouter.Handle, userAndPws []string) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

		// Get the Basic Authentication credentials
		auth := r.Header.Get("Authorization")
		if strings.HasPrefix(auth, basicAuthPrefix) {
			// Check credentials
			payload, err := base64.StdEncoding.DecodeString(auth[len(basicAuthPrefix):])
			if err == nil {
				token := string(payload)
				fmt.Println("xxx", token)
				for _, userAndPw := range userAndPws {
					if userAndPw == token {
						// Delegate request to the given handle
						h(w, r, ps)
						return
					}
				}
			}
		}

		// Request Basic Authentication otherwise
		w.Header().Set("WWW-Authenticate", "Basic realm=WQS")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	}
}

//e.g. router.NotFound(BasicAuthWarp2(http.FileServer(http.Dir(assets)), []string{"admin:admin"}))
func BasicAuthWarp2(h http.Handler, userAndPws []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		auth := r.Header.Get("Authorization")
		if strings.HasPrefix(auth, basicAuthPrefix) {
			payload, err := base64.StdEncoding.DecodeString(auth[len(basicAuthPrefix):])
			if err == nil {
				token := string(payload)
				for _, userAndPw := range userAndPws {
					if userAndPw == token {
						h.ServeHTTP(w, r)
						return
					}
				}
			}
		}

		w.Header().Set("WWW-Authenticate", "Basic realm=WQS")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	})
}
