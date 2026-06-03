package stream

import (
	"net"
	"testing"
)

func TestLocalIPFromAddrsReturnsFirstNonLoopbackIPv4(t *testing.T) {
	addrs := []net.Addr{
		mustCIDR(t, "127.0.0.1/8"),
		mustCIDR(t, "2001:db8::1/64"),
		mustCIDR(t, "192.168.1.10/24"),
		mustCIDR(t, "10.0.0.5/24"),
	}

	got := localIPFromAddrs(addrs)
	if got != "192.168.1.10" {
		t.Fatalf("localIPFromAddrs() = %q, want %q", got, "192.168.1.10")
	}
}

func TestLocalIPFromAddrsReturnsEmptyWhenNoIPv4Exists(t *testing.T) {
	addrs := []net.Addr{
		mustCIDR(t, "127.0.0.1/8"),
		mustCIDR(t, "2001:db8::1/64"),
	}

	if got := localIPFromAddrs(addrs); got != "" {
		t.Fatalf("localIPFromAddrs() = %q, want empty string", got)
	}
}

func mustCIDR(t *testing.T, cidr string) net.Addr {
	t.Helper()

	ip, network, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("ParseCIDR(%q) error = %v", cidr, err)
	}
	network.IP = ip

	return network
}
