FROM devuan-chimaera-rknop

MAINTAINER Rob Knop <raknop@lbl.gov>

RUN apt-get update && apt-get -y upgrade && apt-get -y dist-upgrade
RUN apt-get install -y apache2 python3 libapache2-mod-wsgi-py3 python3-webpy python3-psycopg2

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
