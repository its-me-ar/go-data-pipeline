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
				if strings.Contains(routePath, "/*") {
					// Handle multiple wildcards by converting to regex-like matching
					if matchWildcardRoute(req.URL.Path, routePath) {
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

// matchWildcardRoute checks if a request path matches a wildcard route pattern
func matchWildcardRoute(requestPath, routePattern string) bool {
	// Split both paths into segments
	requestSegments := strings.Split(strings.Trim(requestPath, "/"), "/")
	routeSegments := strings.Split(strings.Trim(routePattern, "/"), "/")

	// Handle single wildcard at the end (matches any number of remaining segments)
	if len(routeSegments) > 0 && routeSegments[len(routeSegments)-1] == "*" {
		// Must have at least as many segments as the route (excluding the wildcard)
		if len(requestSegments) < len(routeSegments)-1 {
			return false
		}

		// Check all segments except the last wildcard
		for i := 0; i < len(routeSegments)-1; i++ {
			if requestSegments[i] != routeSegments[i] {
				return false
			}
		}
		return true
	}

	// Handle exact segment matching (original logic)
	if len(requestSegments) != len(routeSegments) {
		return false
	}

	// Check each segment
	for i, routeSegment := range routeSegments {
		if routeSegment == "*" {
			// Wildcard matches any segment
			continue
		}
		if requestSegments[i] != routeSegment {
			// Exact match required for non-wildcard segments
			return false
		}
	}

	return true
}

// --- Register paths ---
func (r *Router) register(method, path string, handler HandlerFunc) {
	key := method + ":" + path
	r.routes[key] = handler
	r.paths[path] = true
}

func (r *Router) GET(path string, handler HandlerFunc)   { r.register(http.MethodGet, path, handler) }
func (r *Router) POST(path string, handler HandlerFunc)  { r.register(http.MethodPost, path, handler) }
func (r *Router) PUT(path string, handler HandlerFunc)   { r.register(http.MethodPut, path, handler) }
func (r *Router) PATCH(path string, handler HandlerFunc) { r.register(http.MethodPatch, path, handler) }
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
	case http.MethodPatch:
		return colorYellow
	case http.MethodDelete:
		return colorRed
	default:
		return colorCyan
	}
}
