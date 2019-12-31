FROM httpd:2.4

# libreadline for terminal input for debugging
RUN apt-get update && apt-get -y install openssl libssl-dev ca-certificates python3 python3-pip

RUN pip3 install sphinx Flask-Sphinx-Themes

COPY requirements.txt /opt

RUN pip3 install -r /opt/requirements.txt

COPY docker/images/docs/httpd.conf /usr/local/apache2/conf/

COPY docs/ /opt/docs

COPY mreduce /opt/mreduce

WORKDIR /opt

RUN sphinx-apidoc -f -o docs mreduce

WORKDIR /opt/docs

RUN rm -rf /usr/local/apache2/htdocs && ln -s /opt/docs/_build/html /usr/local/apache2/htdocs

RUN make html

RUN chown -R www-data:www-data /usr/local/apache2/htdocs/

WORKDIR /usr/local/apache2/htdocs/

CMD ["httpd-foreground"]
