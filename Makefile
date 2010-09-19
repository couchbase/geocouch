.SUFFIXES: .erl .beam

.erl.beam:
	erlc -I ${COUCH_SRC} -o build -W +debug_info -DTEST $<

MODS = \
    src/vtree/vtree \
    src/geocouch/geocouch_duplicates \
    src/geocouch/couch_httpd_spatial \
    src/geocouch/couch_httpd_spatial_list \
    src/geocouch/couch_spatial \
    src/geocouch/couch_spatial_group \
    src/geocouch/couch_spatial_updater

ERL = erl -boot start_clean

all: builddir compile 

compile: ${MODS:%=%.beam}
	@echo "make clean - clean up"

builddir:
	@mkdir build

clean:	
	rm -rf build
#erl_crash.dump 

