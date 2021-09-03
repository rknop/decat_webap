HOST = http://decat-webap.decat-webap.development.svc.spin.nersc.org
URLDIR = 
WEBAPDIR = /html
INSTALLDIR = /global/cfs/cdirs/m937/decat-webap/html
HOST_DEV = $(HOST)
URLDIR_DEV = /dev
WEBAPDIR_DEV  = /html/dev
INSTALLDIR_DEV = /global/cfs/cdirs/m937/decat-webap/html/dev
PYTHONPATHDIR = /html
WEBAPCONFIG = webapconfig_production
WEBAPCONFIG_DEV = webapconfig_dev

DBDATA = /dbinfo
DBNAME = db
DBNAME_DEV = db_dev

# For this to work, the spin load needs to have keys in a secret mounted at /dbinfo
#   db, db_dev, dbhost, dbpasswd, dbport, dbuser

toinstall = .htaccess decat.css decatview.js decatview.py util.py test.py
webapconfigprod = $(WEBAPCONFIG).py
webapconfigdev = $(WEBAPCONFIG_DEV).py
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
PYTHONPATHDIRP = $(subst /,\/,$(PYTHONPATHDIR))
WEBAPCONFIGP = $(subst /,\/,$(WEBAPCONFIG))
WEBAPCONFIG_DEVP = $(subst /,\/,$(WEBAPCONFIG_DEV))

default:
	@echo Do "make install" or "make dev"

install: webap webapinstall

dev: webapdev webapdevinstall

webapdev: webapconfig.py.in decatview.py.in decatview.js.in
	cat decatview.js.in | perl -pe 's/\@webap\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decatview.py\//' > decatview.js
	cat decatview.py.in | perl -pe 's/\@pythonpathdir\@/$(PYTHONPATHDIRP)/; s/\@webapconfig\@/$(WEBAPCONFIG_DEVP)/;' > decatview.py

webapconfig_dev.py: webapconfig.py.in
	cat webapconfig.py.in | perl -pe 's/\@webapurl\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decatview.py\//; s/\@galapurl\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decat_gal.py\//; s/\@webapdirurl\@/$(URLDIR_DEVP)\//; s/\@webapdir\@/$(WEBAPDIR_DEVP)/; s/\@dbdata\@/$(DBDATAP)/; s/\@dbname\@/$(DBNAME_DEVP)/;' > webapconfig_dev.py

webap: webapconfig.py.in decatview.py.in decatview.js.in
	cat decatview.js.in | perl -pe 's/\@webap\@/$(HOSTP)$(URLDIRP)\/decatview.py\//' > decatview.js
	cat decatview.py.in | perl -pe 's/\@pythonpathdir\@/$(PYTHONPATHDIRP)/; s/\@webapconfig\@/$(WEBAPCONFIGP)/;' > decatview.py

webapconfig_production.py: webapconfig.py.in
	cat webapconfig.py.in | perl -pe 's/\@webapurl\@/$(HOSTP)$(URLDIRP)\/decatview.py\//; s/\@galapurl\@/$(HOSTP)$(URLDIR)\/decat_gal.py\//; s/\@webapdirurl\@/$(URLDIRP)\//; s/\@webapdir\@/$(WEBAPDIRP)/; s/\@dbdata\@/$(DBDATAP)/; s/\@dbname\@/$(DBNAME)/;' > webapconfig_production.py

#webapconfigdev *DOES* go to the production direction.  It's a mess, I know.  Really I should
#  have a dev and a production *server*.
webapdevinstall: $(patsubst %, $(INSTALLDIR_DEV)/%, $(toinstall)) $(patsubst %, $(INSTALLDIR)/%, $(webapconfigdev))

webapinstall: $(patsubst %, $(INSTALLDIR)/%, $(toinstall)) $(patsubst %, $(INSTALLDIR)/%, $(webapconfigprod))

# secretinstall: $(patsubst %, $(SECRETDIR)/%, $(tosecretinstall))

$(INSTALLDIR)/%: %
	cp -p $< $@

$(INSTALLDIR_DEV)/%: %
	cp -p $< $@

# $(SECRETDIR)/%: %
#	cp -p $< $@
