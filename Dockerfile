FROM openresty/openresty

COPY lib/resty/slock.lua /usr/local/openresty/lualib/slock.lua
COPY nginx/nginx.conf /usr/local/openresty/nginx/conf/
COPY nginx/params/*.conf /etc/nginx/
COPY nginx/conf.d/*.conf /etc/nginx/conf.d/

EXPOSE 80 8080