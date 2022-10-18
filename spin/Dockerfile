FROM rknop/devuan-chimaera-rknop

MAINTAINER Rob Knop <raknop@lbl.gov>

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get -y upgrade && apt-get -y dist-upgrade
RUN apt-get install -y apache2 python3 libapache2-mod-wsgi-py3 \
  python3-webpy python3-psycopg2 python3-dateutil python3-tz \
  python3-pil python3-numpy python3-pandas \
  python3-sqlalchemy python3-pycryptodome \
  && apt-get clean

# Patch a bug in web.py that's fixed in the github master but not in
#  devuan chimaera (as of 2021-11-22)
COPY custom/session.py.patch /tmp
RUN patch /usr/lib/python3/dist-packages/web/session.py /tmp/session.py.patch

COPY custom/ports.conf /etc/apache2/
COPY custom/000-default.conf /etc/apache2/sites-available/

# Do scary permissions stuff since we'll have to run
#  as a normal user.  But, given that we're running as
#  a normal user, that makes this less scary.
RUN mkdir -p /var/run/apache2
RUN chmod a+rwx /var/run/apache2
RUN mkdir -p /var/lock/apache2
RUN chmod a+rwx /var/lock/apache2
RUN chmod -R a+rx /etc/ssl/private
RUN mkdir -p /var/log/apache2
RUN chmod -R a+rwx /var/log/apache2

USER 95089
RUN apachectl start

CMD [ "apachectl", "-D", "FOREGROUND", "-D", "APACHE_CONFDIR=/etc/apache2" ]
# CMD "/bin/bash"
