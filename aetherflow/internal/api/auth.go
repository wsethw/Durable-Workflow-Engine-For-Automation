package api

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	roleAdmin    = "admin"
	roleOperator = "operator"
	roleReader   = "reader"

	defaultTenantID = "default"
	principalKey    = "aetherflow.principal"
)

type Options struct {
	APIKeys      []APIKey
	MaxBodyBytes int64
}

type APIKey struct {
	Token    string
	TenantID string
	Role     string
}

type Principal struct {
	TenantID string
	Role     string
}

func ParseAPIKeys(raw string) []APIKey {
	parts := strings.Split(raw, ",")
	keys := make([]APIKey, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key := APIKey{TenantID: defaultTenantID, Role: roleAdmin}
		token, descriptor, ok := strings.Cut(part, "=")
		if ok {
			key.Token = strings.TrimSpace(token)
			tenant, role, hasRole := strings.Cut(strings.TrimSpace(descriptor), ":")
			if strings.TrimSpace(tenant) != "" {
				key.TenantID = strings.TrimSpace(tenant)
			}
			if hasRole && strings.TrimSpace(role) != "" {
				key.Role = normalizeRole(role)
			}
		} else {
			key.Token = part
		}
		if key.Token != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func authenticate(keys []APIKey) gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(keys) == 0 {
			c.Set(principalKey, Principal{TenantID: defaultTenantID, Role: roleAdmin})
			c.Next()
			return
		}
		token := bearerToken(c.GetHeader("Authorization"))
		if token == "" {
			token = c.GetHeader("X-API-Key")
		}
		for _, key := range keys {
			if subtle.ConstantTimeCompare([]byte(token), []byte(key.Token)) == 1 {
				c.Set(principalKey, Principal{TenantID: key.TenantID, Role: key.Role})
				c.Next()
				return
			}
		}
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing API key"})
	}
}

func requireRole(allowed ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		principal := currentPrincipal(c)
		for _, role := range allowed {
			if principal.Role == role {
				c.Next()
				return
			}
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "insufficient role"})
	}
}

func limitBody(maxBytes int64) gin.HandlerFunc {
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	return func(c *gin.Context) {
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		}
		c.Next()
	}
}

func currentPrincipal(c *gin.Context) Principal {
	raw, ok := c.Get(principalKey)
	if !ok {
		return Principal{TenantID: defaultTenantID, Role: roleAdmin}
	}
	principal, ok := raw.(Principal)
	if !ok || principal.TenantID == "" || principal.Role == "" {
		return Principal{TenantID: defaultTenantID, Role: roleAdmin}
	}
	return principal
}

func bearerToken(header string) string {
	prefix := "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, prefix))
}

func normalizeRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case roleReader:
		return roleReader
	case roleOperator:
		return roleOperator
	default:
		return roleAdmin
	}
}
