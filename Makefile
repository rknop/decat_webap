HOST = https://decat-webap.lbl.gov
URLDIR = 
WEBAPDIR = /html
INSTALLDIR = /global/cfs/cdirs/m937/decat-webap/html
HOST_DEV = http://decat-webap.decat-webap.development.svc.spin.nersc.org
URLDIR_DEV =
WEBAPDIR_DEV  = /html
INSTALLDIR_DEV = /global/cfs/cdirs/m937/decat-webap/html_dev
PYTHONPATHDIR = /html

DBDATA = /dbinfo
DBNAME = db
# DBNAME_DEV = db_redo
DBNAME_DEV = db_dev
# DBNAME_DEV = db

# For this to work, the spin load needs to have keys in a secret mounted at /dbinfo
#   db, db_dev, dbhost, dbpasswd, dbport, dbuser

toinstall = .htaccess decat.css decatview.js decatview.py decat_gal.py decatdb.py util.py test.py webapconfig.py
# tosecretinstall = 

toclean = decat_gal.py decatview.js webapconfig.py

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

default:
	@echo Do "make production" or "make dev"

production: webap webapinstall

dev: webapdev webapdevinstall

webapdev: webapconfig.py.in decatview.py decatview.js.in decat_gal.py.in
	cat decatview.js.in | perl -pe 's/\@webap\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decatview.py\//' > decatview.js
	cat decat_gal.py.in | perl -pe 's/\@pythonpathdir\@/$(PYTHONPATHDIRP)/;' > decat_gal.py
	cat webapconfig.py.in | perl -pe 's/\@webapurl\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decatview.py\//; s/\@galapurl\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decat_gal.py\//; s/\@webapdirurl\@/$(URLDIR_DEVP)\//; s/\@webapdir\@/$(WEBAPDIR_DEVP)/; s/\@dbdata\@/$(DBDATAP)/; s/\@dbname\@/$(DBNAME_DEVP)/;' > webapconfig.py

webap: webapconfig.py.in decatview.py decatview.js.in decat_gal.py.in
	cat decatview.js.in | perl -pe 's/\@webap\@/$(HOSTP)$(URLDIRP)\/decatview.py\//' > decatview.js
	cat decat_gal.py.in | perl -pe 's/\@pythonpathdir\@/$(PYTHONPATHDIRP)/;' > decat_gal.py
	cat webapconfig.py.in | perl -pe 's/\@webapurl\@/$(HOSTP)$(URLDIRP)\/decatview.py\//; s/\@galapurl\@/$(HOSTP)$(URLDIR)\/decat_gal.py\//; s/\@webapdirurl\@/$(URLDIRP)\//; s/\@webapdir\@/$(WEBAPDIRP)/; s/\@dbdata\@/$(DBDATAP)/; s/\@dbname\@/$(DBNAME)/;' > webapconfig.py


webapdevinstall: $(patsubst %, $(INSTALLDIR_DEV)/%, $(toinstall))

webapinstall: $(patsubst %, $(INSTALLDIR)/%, $(toinstall))

# secretinstall: $(patsubst %, $(SECRETDIR)/%, $(tosecretinstall))

$(INSTALLDIR)/%: %
	cp -p $< $@

$(INSTALLDIR_DEV)/%: %
	cp -p $< $@

clean: $(toclean)
	rm -vf $^
