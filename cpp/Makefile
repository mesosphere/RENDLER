CXX = g++
CXXFLAGS = -Wall -g -O2 -pthread -std=c++0x
LDFLAGS += -lmesos -lpthread -lprotobuf
CXXCOMPILE = $(CXX) $(INCLUDES) $(CXXFLAGS) -c -o $@
CXXLINK = $(CXX) $(INCLUDES) $(CXXFLAGS) -o $@

default: all
all: rendler crawl_executor render_executor

HEADERS = rendler_helper.hpp


crawl_executor: crawl_executor.cpp $(HEADERS)
	$(CXXLINK) $<  $(LDFLAGS) -lboost_regex -lcurl

%: %.cpp $(HEADERS)
	$(CXXLINK) $< $(LDFLAGS)

check: crawl
	./crawl http://mesosphere.io/team/

clean:
	(rm -f core crawl_executor render_executor rendler)
