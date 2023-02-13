from adoptopenjdk/openjdk8:alpine-slim

RUN apk --no-cache add bash busybox busybox-extras curl
RUN rm -rf /tmp/* /var/cache/apk/*

COPY build/libs/segment_kvrocks_controller-1.0.jar /opt/segment_kvrocks_controller-1.0.jar
WORKDIR /opt
CMD java -jar segment_kvrocks_controller-1.0.jar