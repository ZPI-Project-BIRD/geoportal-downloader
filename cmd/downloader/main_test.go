package main

import "testing"

func TestNewGRPCServerRegistersReflection(t *testing.T) {
	server := newGRPCServer(nil)
	services := server.GetServiceInfo()

	for _, name := range []string{
		"grpc.reflection.v1.ServerReflection",
		"grpc.reflection.v1alpha.ServerReflection",
	} {
		if _, ok := services[name]; !ok {
			t.Errorf("reflection service %q is not registered", name)
		}
	}
}
