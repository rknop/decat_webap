HOST = https://c3.lbl.gov
URLDIR = /raknop/decat/view
INSTALLDIR = /var/www/raknop/decat/view
SECRETS = /home/raknop/secret/dbinfo_production.txt
HOST_DEV = $(HOST)
URLDIR_DEV = /raknop/decat_dev/view
INSTALLDIR_DEV = /var/www/raknop/decat_dev/view
SECRETS_DEV = /home/raknop/secret/dbinfo_dev.txt

# SECRETDIR = /home/raknop/secret

toinstall = .htaccess decat.css decatview.js decatview.py util.py test.py webapconfig.py
# tosecretinstall = 

# ======================================================================

HOSTP = $(subst /,\/,$(HOST))
URLDIRP = $(subst /,\/,$(URLDIR))
INSTALLDIRP = $(subst /,\/,$(INSTALLDIR))
SECRETSP = $(subst /,\/,$(SECRETS))
HOST_DEVP = $(subst /,\/,$(HOST_DEV))
URLDIR_DEVP = $(subst /,\/,$(URLDIR_DEV))
INSTALLDIR_DEVP = $(subst /,\/,$(INSTALLDIR_DEV))
SECRETS_DEVP = $(subst /,\/,$(SECRETS_DEV))

install: webapconfig webapinstall

dev: webapdevconfig webapdevinstall

webapdevconfig: webapconfig.py.in decatview.js.in
	cat webapconfig.py.in | perl -pe 's/\@webapurl\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decatview.py\//; s/\@galapurl\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decat_gal.py\//; s/\@webapdirurl\@/$(URLDIR_DEVP)\//; s/\@webapdir\@/$(INSTALLDIR_DEVP)/; s/\@dbdata\@/$(SECRETS_DEVP)/' > webapconfig.py
	cat decatview.js.in | perl -pe 's/\@webap\@/$(HOST_DEVP)$(URLDIR_DEVP)\/decatview.py\//' > decatview.js
	cat decatview.py.in | perl -pe 's/\@webapdir\@/$(INSTALLDIR_DEVP)/' > decatview.py

webapconfig: webapconfig.py.in decatview.js.in
	cat webapconfig.py.in | perl -pe 's/\@webapurl\@/$(HOSTP)$(URLDIRP)\/decatview.py\//; s/\@galapurl\@/$(HOST)$(URLDIR)\/decat_gal.py\//; s/\@webapdirurl\@/$(URLDIRP)\//; s/\@webapdir\@/$(INSTALLDIRP)/; s/\@dbdata\@/$(SECRETSP)/' > webapconfig.py
	cat decatview.js.in | perl -pe 's/\@webap\@/$(HOSTP)$(URLDIRP)\/decatview.py\//' > decatview.js
	cat decatview.py.in | perl -pe 's/\@webapdir\@/$(INSTALLDIRP)/' > decatview.py

webapdevinstall: $(patsubst %, $(INSTALLDIR_DEV)/%, $(toinstall))

webapinstall: $(patsubst %, $(INSTALLDIR)/%, $(toinstall))

# secretinstall: $(patsubst %, $(SECRETDIR)/%, $(tosecretinstall))

$(INSTALLDIR)/%: %
	cp -p $< $@

$(INSTALLDIR_DEV)/%: %
	cp -p $< $@

# $(SECRETDIR)/%: %
#	cp -p $< $@
