INSTALLDIR = /var/www/raknop/decat/view
SECRETDIR = /home/raknop/secret

toinstall = .htaccess decat.css decatview.js decatview.py util.py test.py webapconfig.py
tosecretinstall = 

install: webapinstall secretinstall

webapinstall: $(patsubst %, $(INSTALLDIR)/%, $(toinstall))

secretinstall: $(patsubst %, $(SECRETDIR)/%, $(tosecretinstall))

$(INSTALLDIR)/%: %
	cp -p $< $@

$(SECRETDIR)/%: %
	cp -p $< $@
