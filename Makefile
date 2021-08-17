HOST = https://decat-webap.decat-webap.development.svc.spin.nersc.org
URLDIR = /exgal
WEBAPDIR = /html/exgal
INSTALLDIR = /global/cfs/cdirs/m937/decat-webap/html/exgal
HOST_DEV = $(HOST)
URLDIR_DEV = /exgal_dev
WEBAPDIR_DEV  = /html/exgal_dev
INSTALLDIR_DEV = /global/cfs/cdirs/m937/decat-webap/html/exgal_dev

DBDATA = /dbinfo
DBNAME = db
DBNAME_DEV = db_dev

# For this to work, the spin load needs to have keys in a secret mounted at /dbinfo
#   db, db_dev, dbhost, dbpasswd, dbport, dbuser

toinstall = .htaccess decat.css decatview.js decatview.py util.py test.py webapconfig.py
# tosecretinstall = 

# ======================================================================

HOSTP = $(subst /,\/,$(HOST))
URLDIRP = $(subst /,\/,$(URLDIR))
WEBAPDIRP = $(subst /,\/,$(WEBAPDIR))
INSTALLDIRP = $(subst /,\/,$(INSTALLDIR))
SECRETSP = $(subst /,\/,$(SECRETS))
HOST_DEVP = $(subst /,\/,$(HOST_DEV))
URLDIR_DEVP = $(subst /,\/,$(URLDIR_DEV))
WEBAPDIR_DEVP = $(subst /,\/,$(WEBAPDIR_DEV))
INSTALLDIR_DEVP = $(subst /,\/,$(INSTALLDIR_DEV))
DBDATAP = $(subst /,\/,$(DBDATA))
DBNAMEP = $(subst /,\/,$(DBNAME))
DBNAME_DEVP = $(subst /,\/,$(DBNAME_DEV))

install: webapconfig webapinstall

dev: webapdevconfig webapdevinstall

webapdevconfig: webapconfig.py.in decatview.js.in
	cat webapconfig.py.in | perl -pe 's/\@webapurl\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decatview.py\//; s/\@galapurl\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decat_gal.py\//; s/\@webapdirurl\@/$(URLDIR_DEVP)\//; s/\@webapdir\@/$(WEBAPDIR_DEVP)/; s/\@dbdata\@/$(DBDATAP)/; s/\@dbname\@/$(DBNAME_DEVP)/;' > webapconfig.py
	cat decatview.js.in | perl -pe 's/\@webap\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decatview.py\//' > decatview.js
	cat decatview.py.in | perl -pe 's/\@webapdir\@/$(WEBAPDIR_DEVP)/' > decatview.py

webapconfig: webapconfig.py.in decatview.js.in
	cat webapconfig.py.in | perl -pe 's/\@webapurl\@/$(HOSTP)$(URLDIRP)\/decatview.py\//; s/\@galapurl\@/$(HOST)$(URLDIR)\/decat_gal.py\//; s/\@webapdirurl\@/$(URLDIRP)\//; s/\@webapdir\@/$(WEBAPDIRP)/; s/\@dbdata\@/$(DBDATAP)/; s/\@dbname\@/$(DBNAME)/;' > webapconfig.py
	cat decatview.js.in | perl -pe 's/\@webap\@/$(HOSTP)$(URLDIRP)\/decatview.py\//' > decatview.js
	cat decatview.py.in | perl -pe 's/\@webapdir\@/$(WEBAPDIRP)/' > decatview.py

webapdevinstall: $(patsubst %, $(INSTALLDIR_DEV)/%, $(toinstall))

webapinstall: $(patsubst %, $(INSTALLDIR)/%, $(toinstall))

# secretinstall: $(patsubst %, $(SECRETDIR)/%, $(tosecretinstall))

$(INSTALLDIR)/%: %
	cp -p $< $@

$(INSTALLDIR_DEV)/%: %
	cp -p $< $@

# $(SECRETDIR)/%: %
#	cp -p $< $@
