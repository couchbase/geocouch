.SUFFIXES: .erl .beam

.erl.beam:
	erlc -I ../couchdb/src/couchdb -o ebin -W +debug_info -DTEST $<

MODS = \
    src/vtree/vtree \
    src/geocouch/geocouch_duplicates \
    src/geocouch/couch_httpd_spatial \
    src/geocouch/couch_httpd_spatial_list \
    src/geocouch/couch_spatial \
    src/geocouch/couch_spatial_group \
    src/geocouch/couch_spatial_updater

ERL = erl -boot start_clean

all: ebin compile 

compile: ${MODS:%=%.beam}
	@echo "make clean - clean up"

ebin:
	@mkdir ebin

clean:	
	rm -rf ebin
#erl_crash.dump 

run:	
	LD_LIBRARY_PATH=/usr/lib/xulrunner-1.9.2.8 ERL_FLAGS="-pa ebin" ../couchdb/utils/run

