CXX = g++
CXXFLAGS = -g -O2
LDFLAGS += -lmesos
CXXCOMPILE = $(CXX) $(INCLUDES) $(CXXFLAGS) -c -o $@
CXXLINK = $(CXX) $(INCLUDES) $(CXXFLAGS) $(LDFLAGS) -o $@

default: all
all: rendler crawl_executor render_executor

HEADERS = rendler_helper.hpp

crawl_executor: crawl_executor.cpp $(HEADERS)
	$(CXXLINK) $< -lboost_regex -lcurl

%: %.cpp $(HEADERS)
	$(CXXLINK) $<

check: crawl
	./crawl http://mesosphere.io/team/

clean:
	(rm -f core crawl_executor render_executor rendler)
