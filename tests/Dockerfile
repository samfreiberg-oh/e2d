FROM amazonlinux:2.0.20210813.1-with-sources

COPY test.sh /usr/local/bin/test.sh

RUN chmod +x /usr/local/bin/test.sh && \
  /usr/local/bin/test.sh -p container
