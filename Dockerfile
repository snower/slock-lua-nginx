FROM openresty/openresty

COPY lib/resty/slock.lua /usr/local/openresty/lualib/slock.lua
COPY conf.d/*.conf /etc/nginx/conf.d/
RUN echo '' >> /usr/local/openresty/nginx/conf/nginx.conf \
    && echo 'env SLOCK_HOSTS;' >> /usr/local/openresty/nginx/conf/nginx.conf \
    && echo 'env SLOCK_HOST;' >> /usr/local/openresty/nginx/conf/nginx.conf \
    && echo 'env SLOCK_PORT;' >> /usr/local/openresty/nginx/conf/nginx.conf \
    && echo 'env REDIS_HOST;' >> /usr/local/openresty/nginx/conf/nginx.conf \
    && echo 'env REDIS_PORT;' >> /usr/local/openresty/nginx/conf/nginx.conf \
    && echo 'env REDIS_DB;' >> /usr/local/openresty/nginx/conf/nginx.conf

EXPOSE 80 8080