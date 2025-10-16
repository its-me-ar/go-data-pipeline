package router

import (
	"log"
	"net/http"
	"strings"
	"time"
)

// --- ANSI color codes ---
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
)

type HandlerFunc func(http.ResponseWriter, *http.Request)

type Router struct {
	mux    *http.ServeMux
	routes map[string]HandlerFunc // key = METHOD:PATH
	paths  map[string]bool        // track registered paths
}

func New() *Router {
	r := &Router{
		mux:    http.NewServeMux(),
		routes: make(map[string]HandlerFunc),
		paths:  make(map[string]bool),
	}

	// Catch-all handler for unknown paths
	r.mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		key := req.Method + ":" + req.URL.Path
		if h, ok := r.routes[key]; ok {
			h(lrw, req)
		} else {
			// Try to find a wildcard route
			found := false
			for routePath := range r.paths {
				if strings.HasSuffix(routePath, "/*") {
					prefix := strings.TrimSuffix(routePath, "/*")
					if strings.HasPrefix(req.URL.Path, prefix+"/") {
						wildcardKey := req.Method + ":" + routePath
						if h, ok := r.routes[wildcardKey]; ok {
							h(lrw, req)
							found = true
							break
						}
					}
				}
			}

			if !found {
				if _, pathExists := r.paths[req.URL.Path]; pathExists {
					// Path exists but method not allowed
					http.Error(lrw, "Method Not Allowed", http.StatusMethodNotAllowed)
				} else {
					// Path not found
					http.Error(lrw, "Not Found", http.StatusNotFound)
				}
			}
		}

		duration := time.Since(start)
		color := statusColor(lrw.statusCode)
		methodColor := methodColor(req.Method)

		log.Printf("%s[%s]%s %s%s%s %s %s%d%s %s(%v)%s",
			colorCyan, start.Format("2006-01-02 15:04:05"), colorReset,
			methodColor, req.Method, colorReset,
			req.URL.Path,
			color, lrw.statusCode, colorReset,
			colorBlue, duration, colorReset,
		)
	})

	return r
}

// --- Register paths ---
func (r *Router) register(method, path string, handler HandlerFunc) {
	key := method + ":" + path
	r.routes[key] = handler
	r.paths[path] = true
}

func (r *Router) GET(path string, handler HandlerFunc)  { r.register(http.MethodGet, path, handler) }
func (r *Router) POST(path string, handler HandlerFunc) { r.register(http.MethodPost, path, handler) }
func (r *Router) PUT(path string, handler HandlerFunc)  { r.register(http.MethodPut, path, handler) }
func (r *Router) DELETE(path string, handler HandlerFunc) {
	r.register(http.MethodDelete, path, handler)
}

// Getter methods for testing
func (r *Router) Routes() map[string]HandlerFunc {
	return r.routes
}

func (r *Router) Paths() map[string]bool {
	return r.paths
}

// --- Start server ---
func (r *Router) Start(addr string) {
	log.Printf("🚀 Server started on %shttp://localhost%s%s", colorGreen, addr, colorReset)
	log.Fatal(http.ListenAndServe(addr, r.mux))
}

// --- Logging response writer to capture status codes ---
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// --- Color helpers ---
func statusColor(code int) string {
	switch {
	case code >= 200 && code < 300:
		return colorGreen
	case code >= 300 && code < 400:
		return colorCyan
	case code >= 400 && code < 500:
		return colorYellow
	default:
		return colorRed
	}
}

func methodColor(method string) string {
	switch method {
	case http.MethodGet:
		return colorGreen
	case http.MethodPost:
		return colorBlue
	case http.MethodPut:
		return colorYellow
	case http.MethodDelete:
		return colorRed
	default:
		return colorCyan
	}
}
