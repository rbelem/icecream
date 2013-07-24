sbin_PROGRAMS = iceccd

iceccd_SOURCES = \
	client.cpp \
	clients.cpp \
	daemon.cpp \
	environment.cpp \
	load.cpp \
	main.cpp \
	ncpus.c \
	serve.cpp \
	workit.cpp

iceccd_LDADD = \
	../services/libicecc.la \
	$(LIB_KINFO) \
	$(CAPNG_LDADD)

AM_CPPFLAGS = \
	-I$(top_srcdir)/services

noinst_HEADERS = \
	environment.h \
	load.h \
	ncpus.h \
	serve.h \
	workit.h
