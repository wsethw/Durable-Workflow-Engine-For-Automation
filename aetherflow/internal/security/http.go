package security

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

var privateHostnames = map[string]struct{}{
	"localhost": {},
}

func ValidateHTTPURL(rawURL string, allowPrivateNetworks bool) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("url must use http or https")
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("url userinfo is not allowed")
	}
	host := parsed.Hostname()
	if strings.TrimSpace(host) == "" {
		return nil, fmt.Errorf("url host is required")
	}
	if !allowPrivateNetworks && isPrivateHost(host) {
		return nil, fmt.Errorf("private or local url host %q is not allowed", host)
	}
	return parsed, nil
}

func EnsureHTTPDestinationAllowed(ctx context.Context, rawURL string, allowPrivateNetworks bool) error {
	parsed, err := ValidateHTTPURL(rawURL, allowPrivateNetworks)
	if err != nil {
		return err
	}
	if allowPrivateNetworks {
		return nil
	}
	host := parsed.Hostname()
	if net.ParseIP(host) != nil {
		return nil
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(lookupCtx, host)
	if err != nil {
		return fmt.Errorf("resolve url host %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return fmt.Errorf("resolve url host %q: no addresses returned", host)
	}
	for _, addr := range addrs {
		if isPrivateIP(addr.IP) {
			return fmt.Errorf("private or local resolved address %s for host %q is not allowed", addr.IP, host)
		}
	}
	return nil
}

func isPrivateHost(host string) bool {
	normalized := strings.TrimSuffix(strings.ToLower(host), ".")
	if _, ok := privateHostnames[normalized]; ok {
		return true
	}
	if strings.HasSuffix(normalized, ".localhost") {
		return true
	}
	ip := net.ParseIP(normalized)
	if ip == nil {
		return false
	}
	return isPrivateIP(ip)
}

func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast()
}
