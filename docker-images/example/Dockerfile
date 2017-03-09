From docker-registry:5000/centos:centos7

#
# Set the version, home directory.
#
ENV UDUP_VERSION=1.0.0 \
    UDUP_HOME=/udup

#
# Create a user and home directory for Udup
#
USER root
RUN groupadd -r udup -g 1001 && useradd -u 1001 -r -g udup -m -d $UDUP_HOME -s /sbin/nologin -c "Udup user" udup && \
    chmod 755 $UDUP_HOME

#
# Download Udup
#
RUN curl -fSL -o udup-$UDUP_VERSION-qa.x86_64.rpm -u ftp:ftp ftp://10.186.18.25/actiontech-udup/qa/$UDUP_VERSION/udup-$UDUP_VERSION-qa.x86_64.rpm

#
# Install ...
#
RUN rpm -ivh udup-$UDUP_VERSION-qa.x86_64.rpm

#
# Change ownership and switch user
#
RUN chown -R udup $UDUP_HOME && \
    chgrp -R udup $UDUP_HOME
USER udup

# Set the working directory to the Udup home directory
WORKDIR $UDUP_HOME

#
# Expose the ports and set up volumes for the data and logs directories
#
EXPOSE 9092
VOLUME ["/udup"]

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["start"]
