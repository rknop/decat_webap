INSTALLDIR = /global/cfs/cdirs/m937/decat-webap/html
INSTALLDIR_DEV = /global/cfs/cdirs/m937/decat-webap/html_dev

# For this to work, the spin load needs to have keys in a secret mounted at /dbinfo
#   db, db_dev, dbhost, dbpasswd, dbport, dbuser

toinstall = .htaccess decat.css decatview.py db.py auth.py decatview_config.py util.py \
	decatview.js decatview_start.js decatview_config.js aes.js jsencrypt.min.js rkauth.js rkwebutil.js \
	exposurelist.js

toclean = decatview_config.js decatview_config.py

# ======================================================================

default:
	@echo Do "make production" or "make dev"

production:
	@echo "Don't"

dev: webapdev webapdevinstall

webapdev: decatview_config_dev.js decatview_config_dev.py
	cp -p decatview_config_dev.js decatview_config.js
	cp -p decatview_config_dev.py decatview_config.py

webap: decatview_config_production.js decatview_config_production.py
	cp -p decatview_config_production.js decatview_config.js
	cp -p decatview_config_production.py decatview_config_production.py

webapdevinstall: $(patsubst %, $(INSTALLDIR_DEV)/%, $(toinstall))

webapinstall: $(patsubst %, $(INSTALLDIR)/%, $(toinstall))

$(INSTALLDIR)/%: %
	cp $< $@

$(INSTALLDIR_DEV)/%: %
	cp $< $@

clean: $(toclean)
	rm -vf $^
