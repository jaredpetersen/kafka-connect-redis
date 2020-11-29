FROM redislabs/redistimeseries:1.4.6 as redistimeseries
FROM redis:6

ENV LIBRARY_PATH /usr/lib/redis/modules

COPY --from=redistimeseries ${LIBRARY_PATH}/redistimeseries.so ${LIBRARY_PATH}/redistimeseries.so

ENTRYPOINT ["redis-server"]
CMD ["/usr/local/etc/redis/redis.conf", "--loadmodule", "/usr/lib/redis/modules/redistimeseries.so"]
