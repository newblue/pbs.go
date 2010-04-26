include $(GOROOT)/src/Make.$(GOARCH)

TARG=pbs
GOFILES=\
	test.pb.go\
	pbs.go

include $(GOROOT)/src/pkg/goprotobuf.googlecode.com/hg/Make.protobuf
include $(GOROOT)/src/Make.pkg
