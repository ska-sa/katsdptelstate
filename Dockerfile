FROM redis:4.0.9-alpine
LABEL maintainer="sdpdev+katsdptelstate@ska.ac.za"

# Change the redis UID/GID to match our Ubuntu containers
RUN deluser redis && addgroup -g 1000 -S redis && adduser -u 1000 -S -G redis redis

COPY redis.conf /usr/local/etc/redis/redis.conf
CMD ["redis-server", "/usr/local/etc/redis/redis.conf"]
